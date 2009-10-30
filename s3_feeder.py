#!/usr/bin/env python
#Recursively feed files and folders into the S3 Upload Queue
# Also has option to only upload missing items

import os, socket, base64

def feed_folder(feed_root, max_depth=1, check_missing=0):
	list_and_feed(feed_root, 1, max_depth)


def list_and_feed(folder, current_depth, max_depth):
	item_list = os.listdir(folder)
	
	#If check missing flag is set, get Amazon Listing Here
		
	folders = []
	for item in item_list:
		if os.path.isdir(item):
			print "Adding Folder to Q: "+str(item)
			folder.append(item)
			
		elif os.path.isfile(item):
			print "Adding file to Q: "+str(item)
		else:
			print "Random item: "+str(item)
	
	
	if current_depth < max_depth:
		for folder in folders:
			list_and_feed(folder, current_depth+1, max_depth)
	
	return 1