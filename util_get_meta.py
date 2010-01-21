#!/usr/bin/env python
#
# Simple CLI front-end for s3_output class
# Get meta info about an object

import s3_config
import s3_output
import sys

try:
	object = str(sys.argv[1])
except:
	raise Exception("You have to define an object path!")

headers = s3_output.get_object_meta(s3_config.s3_bucket, object)

if headers != 0:
	for header in headers:
		print header+": "+headers[header]