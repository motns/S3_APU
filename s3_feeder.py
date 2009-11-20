#!/usr/bin/env python
#Recursively feed files and folders into the S3 Upload Queue
# Also has option to only upload missing items

import os, socket, base64
import s3_config

class Feeder:
	
	# The starting point for feeding files
	feed_root = ""
	
	# The key prefix we'll use when uploading into S3 (without trailing '/')
	key_root = ""
	
	# The maximum depth for recursion
	#	1 = current folder only
	#	0 = Unlimited
	#	> 1 = Go to specified depth
	max_depth = 1
	
	# Whether to Query S3 for a list of files already in place
	check_missing = 0
	
	def __init__ (self, feed_root, key_root, max_depth=1, check_missing=0):
		self.feed_root = feed_root
		self.key_root = key_root
		self.max_depth = max_depth
		self.check_missing = check_missing
	
	def run(self):
		self.list_and_feed(self.feed_root, "", 1)
	
	
	def list_and_feed(self, folder, relative_path, current_depth):
		
		#item_list = [os.path.join(folder,x) for x in os.listdir(folder)]
		
		#If check missing flag is set, get Amazon Listing Here
			
		folders = []
		#s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#try:
		#	s.connect((host, port))
		#except: raise Exception("Failed to connect to Queue server")
		
		for item in os.listdir(folder):
			
			item_with_path = os.path.join(folder,item)
			
			if os.path.isdir(item_with_path):
				work = "mkd|%s" % os.path.join(self.key_root, relative_path, item)
				#s.send(base64.b64encode(work)+"\n")
				#v = base64.b64decode(s.recv(1024))
				
				folders.append(item_with_path)
				
			elif os.path.isfile(item_with_path):
				work = "upl|%s|%s" % (item_with_path, os.path.join(self.key_root, relative_path, item))
				#s.send(base64.b64encode(work)+"\n")
				#v = base64.b64decode(s.recv(1024))

			else:
				print "UNKNOWN item: "+str(item)
			
			print work
		
		#s.close()
		
		
		if self.max_depth == 0 or current_depth < self.max_depth:
			for folder in folders:
				self.list_and_feed(folder, os.path.join(relative_path,os.path.split(folder)[1]), current_depth+1)
		
		return 1