# for the object store
from dulwich.object_store import BaseObjectStore, ShaFile
from cStringIO import StringIO

# for the refstore
from dulwich.repo import RefsContainer

# for the repo
from dulwich.repo import BaseRepo

"""Support for dulwich (git) storage structures on Amazon S3.

This module allows replicating the structure of a git repository on an S3 bucket. This
approach is much lower in overhead then a full fledged file-system, as the core structure
of git, objects, can be translated 1:1 to S3 keys.

The names of the resulting repository is laid in such a way that, if copied over onto
an empty git repository, the result is a valid git repository again.

It is recommend to use this on a non-versioned bucket. A good degree of concurreny can be
achieved with almost no effort: Since uploaded objects are named after their hash, an
object file will always have the same contents if it has its name. Upload the same object
multiple times by accident is therefore not an issue.

When manipulating refs however, you will most likely to implement a locking mechanism.
"""

class S3PrefixFS(object):
	_prefix = ''

	@property
	def prefix(self):
		return self._prefix

	@prefix.setter
	def prefix(self, value):
		# strip leading and trailing slashes, remote whitespace
		self._prefix = value.strip().rstrip('/').lstrip('/').strip()
		# normalize to one trailing '/'
		if self._prefix: self._prefix += '/'


class S3RefsContainer(RefsContainer, S3PrefixFS):
	"""Stores refs in an amazon S3 container.

	Refs are stored in S3 keys the same way as they would on the filesystem, i.e. as
	contents of paths like refs/branches/...

	It is up to to the user of the container to regulate access, as there is no locking
	built-in. While updating a single ref is atomic, doing multiple operations is not."""
	def __init__(self, bucket, prefix = '.git'):
		self.bucket = bucket
		self.prefix = prefix
		super(S3RefsContainer, self).__init__()

	def _calc_ref_path(self, ref):
		return '%s%s' % (self.prefix, ref)

	def allkeys(self):
		path_prefix = '%srefs' % self.prefix
		sublen = len(path_prefix) - 4
		return [k.name[sublen:] for k in self.bucket.get_all_keys(prefix = path_prefix) if not k.name.endswith('/')]

	def read_loose_ref(self, name):
		k = self.bucket.get_key(self._calc_ref_path(name))
		if not k: return None

		return k.get_contents_as_string()

	def get_packed_refs(self):
		return {}

	def set_symbolic_ref(self, name, other):
		# TODO: support symbolic refs
		raise NotImplementedError(self.set_symbolic_ref)

	def set_if_equals(self, name, old_ref, new_ref):
		if old_ref is not None and self.read_loose_ref(name) != old_ref:
			return False

		realname, _ = self._follow(name)

		# set ref (set_if_equals is actually the low-level setting function)
		k = self.bucket.new_key(self._calc_ref_path(name))
		k.set_contents_from_string(new_ref)
		return True

	def add_if_new(self, name, ref):
		if None != self.read_loose_ref(name):
			return False

		self.set_if_equals(name, None, ref)
		return True

	def remove_if_equals(self, name, old_ref):
		k = self.bucket.get_key(self._calc_ref_path(name))
		if None == k: return True

		if old_ref is not None and k.get_contents_as_string() != old_ref:
			return False

		k.delete()
		return True


class S3ObjectStore(BaseObjectStore, S3PrefixFS):
	"""Storage backend on an Amazon S3 bucket.

	Stores objects on S3, replicating the path structure found usually on a "real"
	filesystem-based repository. Does not support packs."""

	def __init__(self, bucket, prefix = '.git'):
		super(S3ObjectStore, self).__init__()
		self.bucket = bucket
		self.prefix = prefix

	def contains_loose(self, sha):
		"""Check if a particular object is present by SHA1 and is loose."""
		return bool(self.bucket.get_key(calc_object_path(self.prefix, sha)))

	def contains_packed(self, sha):
		"""Check if a particular object is present by SHA1 and is packed."""
		return False

	def __iter__(self):
		path_prefix = '%sobjects/' % self.prefix
		path_prefix_len = len(path_prefix)

		# valid keys look likes this: "path_prefix + 2 bytes sha1 digest + /
		#                              + remaining 38 bytes sha1 digest"
		valid_len = path_prefix_len + 2 + 1 + 38
		return (k.name[-41:-39] + k.name[-38:] for k in self.bucket.get_all_keys(prefix = path_prefix) if len(k.name) == valid_len)

	@property
	def packs(self):
		return []

	def __getitem__(self, name):
		# create ShaFile from downloaded contents
		k = self.bucket.new_key(calc_object_path(self.prefix, name))
		buf = k.get_contents_as_string()

		return ShaFile.from_file(StringIO(buf))

	def get_raw(self, name):
		ret = self[name]
		return ret.type_num, ret.as_raw_string()

	def add_object(self, obj):
		"""Adds object the repository. Adding an object that already exists will
		   still cause it to be uploaded, overwriting the old with the same data."""
		k = self.bucket.new_key(calc_object_path(self.prefix, obj.sha().hexdigest()))

		# add metadata
		k.set_metadata('type_num', str(obj.type_num))
		k.set_metadata('raw_length', str(obj.raw_length()))

		# actual upload
		k.set_contents_from_string(obj.as_legacy_object())

	def add_objects(self, objects):
		for obj, path in objects:
			self.add_object(obj)

class S3Repo(BaseRepo):
	"""A dulwich repository stored in an S3 bucket. Uses S3RefsContainer and S3ObjectStore
	as a backend. Does not do any sorts of locking, see documentation of S3RefsContainer
	and S3ObjectStore for details."""
	def __init__(self, bucket, prefix = '.git'):
		object_store = S3ObjectStore(bucket, prefix)
		refs = S3RefsContainer(bucket, prefix)
		super(S3Repo, self).__init__(object_store, refs)


def calc_object_path(prefix, hexsha):
	# FIXME: make this a method again?
	path = '%sobjects/%s/%s' % (prefix, hexsha[0:2], hexsha[2:40])
	return path
