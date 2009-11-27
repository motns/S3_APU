#!/usr/bin/env python
import os, sys
import s3_feeder

#Get and validate feed path
try:
	feed_root = sys.argv[1]
	print feed_root
	if not os.path.exists(feed_root) or not os.path.isdir(feed_root):
		raise Exception("Invalid path")
except: raise Exception("You have to pass in a valid folder path as the first argument")

#The key root (or prefix) to use for uploading
try:
	key_root = str(sys.argv[2])
except: key_root = ""

#Recursion depth
try:
	depth = int(sys.argv[3])
except: depth = 1

#Checking against S3 for missing items
try:
	check_missing = int(sys.argv[4])
except: check_missing = 0

Feeder = s3_feeder.Feeder(feed_root, key_root, depth, check_missing)
Feeder.run()