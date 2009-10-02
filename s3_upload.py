#!/usr/bin/env python
import s3_config
import s3_signature
import base64
import pycurl
import time
import os
import cStringIO
import hashlib
from xml.dom import minidom

def upload_object(bucket,instruction,destination_key="",source_path=""):

	if bucket.strip() == "": raise Exception("You have to pass in a bucket name")
	if destination_key.strip() == "": raise Exception("You have to pass in a destination key")
	
	#Set content type, and check file
	if instruction == "upl":
		#content_type = "image/jpeg"
		content_type = "text/plain"
		meta_mode = int(0100775)
		
		if os.path.exists(source_path) == False: raise Exception("The file specified doesn't exist")
		
		#Get checksum
		checksum = base64.b64encode(
			hashlib.md5(
				open(source_path, 'rb').read()
			).digest()
		)
		
	elif instruction == "mkd":
		content_type = "application/x-directory"
		meta_mode = int(040775)
		
		checksum = ""
	else:
		raise Exception("Invalid instruction")
	
	#Build Base URI
	uri = "http://"+bucket+".s3.amazonaws.com/"+destination_key
	
	
	#Create Headers
	headers = {
		'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime()),
		'User-Agent':'S3 Python Uploader',
		'Content-Type':content_type,
		'x-amz-acl':'public-read',
		'x-amz-meta-gid': str(s3_config.upload_gid),
		'x-amz-meta-mode': str(meta_mode),
		'x-amz-meta-mtime': str(int(time.time())),
		'x-amz-meta-uid': str(s3_config.upload_uid)
	}
	
	if checksum != "": headers['Content-MD5'] = checksum
	
	headers['Authorization'] = s3_signature.get_auth_header('PUT', '/'+bucket+'/'+destination_key, headers)
	
	#Initiate curl object
	c = pycurl.Curl()
	c.setopt(pycurl.URL, uri)
	c.setopt(pycurl.HTTPHEADER, [h+": "+str(headers[h]) for h in headers])
	c.setopt(pycurl.VERBOSE, 0)
	c.setopt(pycurl.HEADER, 1)
	c.setopt(pycurl.UPLOAD, 1)
	
	#For uploads only
	if instruction == "upl":
	
		#Read file for upload
		c.setopt(pycurl.READFUNCTION, open(source_path, 'rb').read)
		
		# Set size of file to be uploaded.
		filesize = os.path.getsize(source_path)
		c.setopt(pycurl.INFILESIZE, filesize)
		
	elif instruction == "mkd":
		
		#Fake empty file object
		fake_file = cStringIO.StringIO()
		
		#Read file for upload
		c.setopt(pycurl.READFUNCTION, fake_file.read)
		
		# Set size of file to be uploaded.
		c.setopt(pycurl.INFILESIZE, 0)
	
	
	#Catch response
	res = cStringIO.StringIO()
	c.setopt(pycurl.WRITEFUNCTION, res.write)
		
	#Do It
	c.perform()
	
	#Error handling
	#print c.getinfo(pycurl.HTTP_CODE)
	
	c.close()
	
	print res.getvalue()