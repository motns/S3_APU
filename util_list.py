#!/usr/bin/env python
#
# Simple CLI front-end for s3_output class
# Lists keys with a certain prefix

import s3_config
import s3_output
import os,sys

try:
	prefix = str(sys.argv[1])
except:
	prefix = ""

try:
	limit = int(sys.argv[2])
except: limit = 0
	
#Little hack for convenience
# Will use the first argument as 'limit', if it doesn't look like a path
if prefix != "" and prefix[-1] != "/":
	try:
		limit = int(prefix)
		prefix = ""
	except: pass
	
for k in s3_output.list_objects(s3_config.s3_bucket, os.path.join(prefix, ''), "", limit, "/"):
	print k