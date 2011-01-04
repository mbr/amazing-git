# for the object store
from dulwich.object_store import BaseObjectStore, ShaFile
from cStringIO import StringIO

# for the refstore
from dulwich.repo import RefsContainer

class AmazonS3RefsContainer(RefsContainer):
	def __init__(self, bucket, prefix = '.git/'):
		self.bucket = bucket
		self.prefix = prefix.rstrip('/')

	def _calc_ref_path(self, ref):
		return '%s/%s' % (self.prefix, ref)

	def allkeys(self):
		path_prefix = '%s/refs' % self.prefix
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


class AmazonS3ObjectStore(BaseObjectStore):
	"""Uses Amazon S3 as the storage backend (through boto)."""

	def __init__(self, bucket, prefix = '.git/'):
		super(AmazonS3ObjectStore, self).__init__()
		self.bucket = bucket
		self.prefix = prefix.rstrip('/')

	def contains_loose(self, sha):
		"""Check if a particular object is present by SHA1 and is loose."""
		return bool(self.bucket.get_key(self._calc_object_path(sha)))
		
	def contains_packed(self, sha):
		"""Check if a particular object is present by SHA1 and is packed."""
		return False

	def __iter__(self):
		path_prefix = '%s/objects/' % self.prefix
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
		k = self.bucket.new_key(self._calc_object_path(name))
		buf = k.get_contents_as_string()

		return ShaFile.from_file(StringIO(buf))

	def get_raw(self, name):
		ret = self[name]
		return ret.type_num, ret.as_raw_string()

	def add_object(self, obj):
		"""Adds object the repository. Adding an object that already exists will
		   still cause it to be uploaded, overwriting the old with the same data."""
		k = self.bucket.new_key(self._calc_object_path(obj.sha().hexdigest()))
		print k

		# actual upload
		k.set_contents_from_string(obj.as_legacy_object())

	def add_objects(self, objects):
		for obj in objects:
			self.add_object(obj)

	def _calc_object_path(self, hexsha):
		path = '%s/objects/%s/%s' % (self.prefix, hexsha[0:2], hexsha[2:40])
		return path
