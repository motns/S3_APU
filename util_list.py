#!/usr/bin/env python
#
# Simple CLI front-end for s3_listing class
# Lists keys with a certain prefix

import s3_config
import s3_listing
import os,sys

try:
	prefix = os.path.join(sys.argv[1],"")
except:
	prefix = ""

try:
	limit = int(sys.argv[2])
except: limit = 0
	
for k in s3_listing.list_objects(s3_config.s3_bucket, prefix, "", limit, "/"):
	print k