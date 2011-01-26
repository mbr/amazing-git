#!/usr/bin/env python
# coding=utf8

import dateutil.parser
import time
import uuid

from boto.s3.key import Key
from boto.s3.deletemarker import DeleteMarker

import logbook

log = logbook.Logger('S3VersionLock')
debug = log.debug
info = log.info

def cmp_by_timestamp(a, b):
	"""Compare two S3 keys by timestamp, ascending."""
	return cmp(dateutil.parser.parse(a.last_modified), dateutil.parser.parse(b.last_modified))

def has_versioning(bucket):
	"""Returns 'True' if bucket has versioning, False otherwise."""
	vers = bucket.get_versioning_status()
	return 'Versioning' in vers and vers['Versioning'] == 'Enabled'


def get_ordered_versions(bucket, path):
	"""Get all versions of a path on an S3 bucket, ordered by their 'last-modified'
	   timestamp in ascending order."""
	# check all versions, check if path matches
	# note: DeleteMarkers do not have key attribute, rather a 'name' attribute
	#       these are merged if they are next to each other and drop out on either end
	#       on the version list. however, the last delete marker is kept
	keys = [k for k in bucket.get_all_versions(prefix = path) if hasattr(k, 'key') and k.key == path or k.name == path]

	# sort by timestamp, ascending
	keys.sort(cmp_by_timestamp)

	return keys


def filter_delete_markers(l):
	"""Remove all S3 DeleteMarker instances from a list (returns
	   a generator."""
	for i in l:
		if isinstance(i, DeleteMarker): continue
		yield i


class S3VersionLock(object):
	"""S3 Lock based on versioning.

	A locking mechanism for amazon S3 that leverages the versioning feature to create
	locks. A new version of a file is uploaded first, then checks if the oldest
	non-deleted version is the same as the one uploaded. Once it 'acquires' the lock
	this way, runs the critical section, the removes its version."""

	def __init__(self, bucket, name, interval = 0.5):
		self.bucket = bucket
		self.name = name
		self.interval = interval
		debug('New lock named %s instantiated on %r' % (self.name, self.bucket))
		assert(has_versioning(bucket))

	def __enter__(self):
		info('Trying to acquire %s' % self.name)

		self.lock_key = Key(self.bucket)
		self.lock_key.key = self.name

		self.lock_key.set_contents_from_string('')
		# version id of self.lock_key is now the one we set
		debug('Uploaded lock request, version id %s' % self.lock_key.version_id)

		while True:
			keys = list(filter_delete_markers(get_ordered_versions(self.bucket, self.lock_key.key)))
			debug('Lock-queue: *%s' % ', '.join((k.version_id for k in keys)))

			if keys[0].version_id == self.lock_key.version_id:
				info('Acquired %s' % self.name)
				break

			debug('Could not acquire lock, sleeping for %s seconds' % self.interval)
			time.sleep(self.interval)

		# we hold the lock, code runs

	def __exit__(self, type, value, traceback):
		# release the lock
		self.lock_key.delete()
		info('Released lock %s on %r' % (self.name, self.bucket))


class S3KeyLock(object):
	"""S3 Lock based on timestamps.

	This S3 locking mechanism relies on S3 timestamps being synchronized
	and read-after-write consistency. To acquire a lock, a file with a
	random ID is created. Afterwards, all possible lock files are retrieved
	and if the random ID that was generated is the one with the lowest
	timestamp, we have the lock. Otherwise wait for others to finish their
	work and release their lock.

	Works well on non-versionend buckets, could work, but not tested on,
	buckets with versioning enabled.
	"""
	def __init__(self, bucket, prefix = '', interval = 0.5):
		self.bucket = bucket
		self.prefix = prefix
		assert(not has_versioning(self.bucket))
		self.interval = 0.5

	def __enter__(self):
		debug('Trying to acquire %s on %r' % (self.prefix, self.bucket))

		# generate a UUID
		lock_id = uuid.uuid1()
		debug('Lock ID: %s' % lock_id)

		# create file
		self.lock_key = Key(self.bucket)
		self.lock_key.key = '%s/%s.lock' % (self.prefix, lock_id)
		self.lock_key.set_contents_from_string('')

		# at this point, it's possible to maybe get around eventual consistency
		# by waiting for out file to appear? only the amazons know!

		while True:
			keys = [k for k in self.bucket.get_all_keys(prefix = self.prefix) if k.key.endswith('.lock')]

			# sort by timestamp, ascending
			keys.sort(cmp_by_timestamp)
			debug('Lock-queue: *%s' % ', '.join((k.key for k in keys)))

			if keys[0].key == self.lock_key.key:
				info('Acquired %s' % self.prefix)
				break

			debug('Could not acquire lock, sleeping for %s seconds' % self.interval)
			time.sleep(self.interval)

	def __exit__(self, type, value, traceback):
		self.lock_key.delete()
		info('Released lock %s on %r' % (self.lock_key.key, self.bucket))

if '__main__' == __name__:
	from secretkey import *
	from boto.s3.connection import S3Connection
	import sys

	conn = S3Connection(key_id, access_key)
	bucketname = 'mbr-nvbucket'

	# get bucket
	bucket = conn.get_bucket(bucketname)

	with S3KeyLock(bucket, 'my_amazing_lock'):
		print "RUNNING CRITICAL SECTION"
		print "Press enter to end critical section"
		sys.stdin.readline()
