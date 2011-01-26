#!/usr/bin/env python
# coding=utf8

from secretkey import *

from boto.s3.connection import S3Connection
from boto.s3.key import Key
from boto.exception import S3ResponseError

import dateutil.parser

import uuid

import time

conn = S3Connection(key_id, access_key)

def has_versioning(bucket):
	vers = bucket.get_versioning_status()
	return 'Versioning' in vers and vers['Versioning'] == 'Enabled'

bucketname = 'mbr-locktest_2'

# delete and create bucket
#bucket = conn.get_bucket(bucketname)
#keys = bucket.get_all_versions()
#print keys
#conn.delete_bucket(bucketname)
#bucket = conn.create_bucket(bucketname)
#bucket.configure_versioning(True)
#print "BUCKET RECREATED"

# get bucket
bucket = conn.get_bucket(bucketname)
assert(has_versioning(bucket))

# create the key
lock_key = Key(bucket)
lock_key.key = 'testing_key'

# no uuids required - use the version key as the id
#lock_id = uuid.uuid1()
#print "lock_id: %s" % lock_id
#lock_key.set_contents_from_string(lock_id)

lock_key.set_contents_from_string('')
print "version_id: %s" % lock_key.version_id

while True:
	# check all versions
	keys = bucket.get_all_versions(prefix = lock_key.key)

	# FIXME: check key name

	# sort by timestamp, ascending
	keys.sort(lambda a, b: cmp(dateutil.parser.parse(a.last_modified), dateutil.parser.parse(b.last_modified)))

	# check oldest version
	print "oldest key:",keys[0].version_id
	if keys[0].version_id == lock_key.version_id:
		print "WE GOT THE LOCK"
		break

	print "sleeping, waiting for lock to be available..."
	time.sleep(1)

print "running critical section"
# critical section goes here

# clear lock
lock_key.delete()
