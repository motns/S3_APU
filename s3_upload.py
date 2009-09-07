#!/usr/bin/env python
import s3_signature
import base64
import pycurl
import time
import os
import cStringIO
from xml.dom import minidom

def upload_object(bucket,source_path="",destination_key="",content_type="text/plain",amz_acl="public-read"):

	if bucket.strip() == "": raise "You have to pass in a bucket name"
	if destination_key.strip() == "": raise "You have to pass in a destination key"
	if os.path.exists(source_path) == False: raise "The file specified doesn't exist"
	
	#Build Base URI
	uri = "http://"+bucket+".s3.amazonaws.com"
	
	#Get checksum
	h = os.popen(r"md5 -q "+file_path);
	checksum = base64.b64encode(h.read().strip())
	
	#Create Headers
	headers = {
		'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime()),
		'User-Agent':'S3 Python API',
		'Content-MD5':checksum,
		'Content-Type':content_type,
		'x-amz-acl':amz_acl
	}
	headers['Authorization'] = s3_signature.get_auth_header('PUT', '/'+bucket+'/'+destination_key, headers)
	
	#Initiate curl object
	c = pycurl.Curl()
	c.setopt(pycurl.URL, uri)
	c.setopt(pycurl.HTTPHEADER, [h+": "+str(headers[h]) for h in headers])
	c.setopt(pycurl.VERBOSE, 0)
	c.setopt(pycurl.HEADER, 1)
	
	#Catch response
	res = cStringIO.StringIO()
	c.setopt(pycurl.WRITEFUNCTION, res.write)
		
	#Do It
	c.perform()
	c.close()
	
	print res.getvalue()