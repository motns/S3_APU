#!/usr/bin/env python
#
# Simple CLI front-end for s3_output class
# Lists keys with a certain prefix

import s3_config
import s3_output
import os, re, sys

try:
	prefix = str(sys.argv[1])
except:
	prefix = ""

try:
	limit = int(sys.argv[2])
except: limit = 0
	
#Little hack for convenience
# Will use the first argument as 'limit', if it doesn't look like a path
if prefix != "" and prefix[-1] != "/" and len(sys.argv) == 2:
	try:
		limit = int(prefix)
		prefix = ""
	except: pass
	
try:
	bucket = str(sys.argv[3]).strip()
	if re.match('[a-zA-Z0-9\-\_\.]+$', bucket) == None:
		print "Invalid bucket name! Falling back to default."
		raise Exception("Invalid")
except:
	bucket = s3_config.s3_bucket

	
out = s3_output.list_objects(bucket, prefix, "", limit, "/")
if out != None and out != 0:
	for k in out:
		print k