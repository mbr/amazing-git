from binascii import hexlify, unhexlify
import os
import tempfile
import time
import zlib

import threading
from Queue import Queue

# for the object store
from dulwich.object_store import PackBasedObjectStore, ShaFile, ObjectStoreIterator
from dulwich.objects import Blob
from dulwich.pack import PackData, iter_sha1, write_pack_index_v2, Pack, load_pack_index_file
from cStringIO import StringIO

# for the refstore
from dulwich.repo import RefsContainer, SYMREF

# for the repo
from dulwich.repo import BaseRepo

import logbook
log = logbook.Logger('git-remote-s3')

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

When manipulating refs however, you will most likely need to implement a locking mechanism.
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
	def __init__(self, create_bucket, prefix = '.git'):
		self.bucket = create_bucket()
		self.prefix = prefix
		super(S3RefsContainer, self).__init__()

	def _calc_ref_path(self, ref):
		return '%s%s' % (self.prefix, ref)

	def allkeys(self):
		path_prefix = '%srefs' % self.prefix
		sublen = len(path_prefix) - 4
		refs = [k.name[sublen:] for k in self.bucket.get_all_keys(prefix = path_prefix) if not k.name.endswith('/')]
		if self.bucket.get_key(self._calc_ref_path('HEAD')): refs.append('HEAD')
		return refs

	def read_loose_ref(self, name):
		k = self.bucket.get_key(self._calc_ref_path(name))
		if not k: return None

		return k.get_contents_as_string()

	def get_packed_refs(self):
		return {}

	def set_symbolic_ref(self, name, other):
		sref = SYMREF + other
		log.debug('setting symbolic ref %s to %r' % (name, sref))
		k = self.bucket.new_key(self._calc_ref_path(name))
		k.set_contents_from_string(sref)

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


class S3ObjectStore(PackBasedObjectStore, S3PrefixFS):
	"""Storage backend on an Amazon S3 bucket.

	Stores objects on S3, replicating the path structure found usually on a "real"
	filesystem-based repository. Does not support packs."""

	def __init__(self, create_bucket, prefix = '.git', num_threads = 16):
		super(S3ObjectStore, self).__init__()
		self.bucket = create_bucket()
		self.create_bucket = create_bucket
		self.prefix = prefix
		self.uploader_threads = []
		self.work_queue = Queue()

		self._pack_cache_time = 0

	def add_pack(self):
		fd, path = tempfile.mkstemp(suffix = ".pack")
		f = os.fdopen(fd, 'wb')

		def commit():
			try:
				os.fsync(fd)
				f.close()

				return self.upload_pack_file(path)
			finally:
				os.remove(path)
				log.debug('Removed temporary file %s' % path)
		return f, commit

	def _create_pack(self, path):
		def data_loader():
			# read and writable temporary file
			pack_tmpfile = tempfile.NamedTemporaryFile()

			# download into temporary file
			log.debug('Downloading pack %s into %s' % (path, pack_tmpfile))
			pack_key = self.bucket.new_key('%s.pack' % path)

			# store
			pack_key.get_contents_to_file(pack_tmpfile)
			log.debug('Filesize is %d' % pack_key.size)

			log.debug('Rewinding...')
			pack_tmpfile.flush()
			pack_tmpfile.seek(0)

			return PackData.from_file(pack_tmpfile, pack_key.size)

		def idx_loader():
			index_tmpfile = tempfile.NamedTemporaryFile()

			log.debug('Downloading pack index %s into %s' % (path, index_tmpfile))
			index_key = self.bucket.new_key('%s.idx' % path)

			index_key.get_contents_to_file(index_tmpfile)
			log.debug('Rewinding...')
			index_tmpfile.flush()
			index_tmpfile.seek(0)

			return load_pack_index_file(index_tmpfile.name, index_tmpfile)

		p = Pack(path)

		p._data_load = data_loader
		p._idx_load = idx_loader

		return p

	def contains_loose(self, sha):
		"""Check if a particular object is present by SHA1 and is loose."""
		return bool(self.bucket.get_key(calc_object_path(self.prefix, sha)))

	def upload_pack_file(self, path):
		p = PackData(path)
		entries = p.sorted_entries()

		# get the sha1 of the pack, same method as dulwich's move_in_pack()
		pack_sha = iter_sha1(e[0] for e in entries)
		key_prefix = calc_pack_prefix(self.prefix, pack_sha)
		pack_key_name = '%s.pack' % key_prefix

		# FIXME: LOCK HERE? Possibly different pack files could
		#        have the same shas, depending on compression?

		log.debug('Uploading %s to %s' % (path, pack_key_name))

		pack_key = self.bucket.new_key(pack_key_name)
		pack_key.set_contents_from_filename(path)
		index_key_name = '%s.idx' % key_prefix

		index_key = self.bucket.new_key(index_key_name)

		index_fd, index_path = tempfile.mkstemp(suffix = '.idx')
		try:
			f = os.fdopen(index_fd, 'wb')
			write_pack_index_v2(f, entries, p.get_stored_checksum())
			os.fsync(index_fd)
			f.close()

			log.debug('Uploading %s to %s' % (index_path, index_key_name))
			index_key.set_contents_from_filename(index_path)
		finally:
			os.remove(index_path)

		p.close()

		return self._create_pack(key_prefix)

	def __iter__(self):
		return (k.name[-41:-39] + k.name[-38:] for k in self._s3_keys_iter())

	def _pack_cache_stale(self):
		# pack cache is valid for 5 minutes - no fancy checking here
		return time.time() - self._pack_cache_time > 5*60

	def _load_packs(self):
		packs = []

		# return pack objects, replace _data_load/_idx_load
		# when data needs to be fetched
		log.debug('Loading packs...')
		for key in self.bucket.get_all_keys(prefix = '%sobjects/pack/' % self.prefix):
			if key.name.endswith('.pack'):
				log.debug('Found key %r' % key)
				packs.append(self._create_pack(key.name[:-len('.pack')]))

		self._pack_cache_time = time.time()
		return packs

	def _s3_keys_iter(self):
		path_prefix = '%sobjects/' % self.prefix
		path_prefix_len = len(path_prefix)

		# valid keys look likes this: "path_prefix + 2 bytes sha1 digest + /
		#                              + remaining 38 bytes sha1 digest"
		valid_len = path_prefix_len + 2 + 1 + 38
		return (k for k in self.bucket.get_all_keys(prefix = path_prefix) if len(k.name) == valid_len)

	def add_object(self, obj):
		"""Adds object the repository. Adding an object that already exists will
		   still cause it to be uploaded, overwriting the old with the same data."""
		self.add_objects([obj])


class S3CachedObjectStore(S3ObjectStore):
	def __init__(self, *args, **kwargs):
		super(S3CachedObjectStore, self).__init__(*args, **kwargs)
		self.cache = {}

	def __getitem__(self, name):
		if name in self.cache:
			log.debug('Cache hit on %s' % name)
			return self.cache[name]

		obj = super(S3CachedObjectStore, self).__getitem__(name)
		# do not store blobs
		if obj.get_type() == Blob.type_num:
			log.debug('Not caching Blob %s' % name)
		else:
			self.cache[obj.id] = obj

		return obj


class S3Repo(BaseRepo):
	"""A dulwich repository stored in an S3 bucket. Uses S3RefsContainer and S3ObjectStore
	as a backend. Does not do any sorts of locking, see documentation of S3RefsContainer
	and S3ObjectStore for details."""
	def __init__(self, create_bucket, prefix = '.git'):
		object_store = S3CachedObjectStore(create_bucket, prefix)
		refs = S3RefsContainer(create_bucket, prefix)

		# check if repo is initialized
		super(S3Repo, self).__init__(object_store, refs)

		try:
			log.debug('S3Repo with HEAD %r' % refs['HEAD'])
		except KeyError:
			self._init()

	def _init(self):
		log.debug('Initializing S3 repository')
		self.refs.set_symbolic_ref('HEAD', 'refs/heads/master')


def calc_object_path(prefix, hexsha):
	path = '%sobjects/%s/%s' % (prefix, hexsha[0:2], hexsha[2:40])
	return path

def calc_pack_prefix(prefix, hexsha):
	path = '%sobjects/pack/pack-%s' % (prefix, hexsha)
	return path

def calc_path_id(prefix, path):
	hexsha = path[-41:-39] + path[-38:]
	return hexsha
