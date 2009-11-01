#!/usr/bin/env python
#Recursively feed files and folders into the S3 Upload Queue
# Also has option to only upload missing items

import os, socket, base64

class Feeder:
	
	feed_root = ""
	max_depth = 1
	check_missing = 0
	
	def __init__ (self, feed_root, max_depth=1, check_missing=0):
		self.feed_root = feed_root
		self.max_depth = max_depth
		self.check_missing = check_missing
	
	def run(self):
		self.list_and_feed(self.feed_root, 1)
	
	
	def list_and_feed(self, folder, current_depth):
		
		item_list = [os.path.join(folder,x) for x in os.listdir(folder)]
		
		#If check missing flag is set, get Amazon Listing Here
			
		folders = []
		for item in item_list:
			
			if os.path.isdir(item):
				print ((current_depth - 1) * "\t")+"Adding Folder to Q: "+str(item)
				folders.append(item)
				
			elif os.path.isfile(item):
				print ((current_depth - 1) * "\t")+"Adding file to Q: "+str(item)
			else:
				print "UNKNOWN item: "+str(item)
		
		
		if current_depth < self.max_depth:
			for folder in folders:
				self.list_and_feed(folder, current_depth+1)
		
		return 1