from dulwich.object_store import BaseObjectStore, ShaFile
from cStringIO import StringIO

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
