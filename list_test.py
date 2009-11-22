#!/usr/bin/env python

import s3_listing
import s3_config

keys = s3_listing.list_objects(s3_config.s3_bucket,"s3_test/","",10,"/")

for key in keys:
	print key