#!/usr/bin/env python
import s3_config
import s3_signature
import pycurl
import time, cStringIO
from xml.dom import minidom

def list_objects(bucket,prefix="",marker="",maxkeys=10,delimiter=""):

	if bucket.strip() == "": raise "You have to pass in a bucket name"

	#Build Base URI
	uri = "http://"+bucket+".s3.amazonaws.com"
	
	#Check for params
	params = {}
	if(prefix.strip != ""): params['prefix'] = prefix
	if(marker.strip != ""): params['marker'] = marker
	if(maxkeys is int and maxkeys != 0):
		params['maxkeys'] = maxkeys
	else:
		params['maxkeys'] = 10
	if(delimiter.strip != ""): params['delimiter'] = delimiter
	
	#Append params to URI
	if len(params) > 0:
		uri += "?"
		p = []
		for param in params:
			p.append(param+"="+str(params[param]))
		uri += "&".join(p)
	
	#Create Headers
	headers = {
		'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.localtime()),
		'User-Agent':'S3 Python API'
	}
	headers['Authorization'] = s3_signature.get_auth_header('GET', '/'+bucket+'/', headers)
	
	
	#Repeat transaction until successful, or we run out of retries
	retries = 0
	while 1:
		c = pycurl.Curl()
		c.setopt(pycurl.URL, uri)
		c.setopt(pycurl.HTTPHEADER, [h+": "+str(headers[h]) for h in headers])
		c.setopt(pycurl.VERBOSE, 0)
		c.setopt(pycurl.HEADER, 0)
		
		#Catch response
		res = cStringIO.StringIO()
		c.setopt(pycurl.WRITEFUNCTION, res.write)
		
		#Do It
		retry = 0
		c.perform()
		
		ret_code = int(c.getinfo(pycurl.RESPONSE_CODE))
		
		if ret_code in [200]: pass #Success
		elif ret_code in [400,403,405,411,412,501]: retry = 1 #We must have messed up the Request (Not Recoverable)
		elif ret_code == 500: retry = 1
		elif ret_code == 503: retry = 1 #Wow,wow...Hold your horses! We probably hit a SlowDown
		else: retry = 1 #Empty response (DNS/Connect timeout perhaps?)
		
		c.close()
		
		if retry == 0: break
		elif retry == 1 and retries <= s3_config.max_retries:
			retries += 1
			print "cURL transaction for S3 Listing failed. Retrying...\n"
			time.sleep(3)
		
		else:
			print "cURL transaction for S3 Listing failed too many times. Giving up...\n"
			return 0


	return_keys = []
	dom = minidom.parseString(res.getvalue())
	for key in dom.getElementsByTagName('Key'):
		return_keys.append(key.firstChild.nodeValue)
	
	return return_keys
	
	
################################################################################
################################################################################

def get_object(bucket,object_key=""):

	if bucket.strip() == "": raise "You have to pass in  a bucket name"
	if object_key.strip() == "": raise "You have to pass in an object key"

	#Build Base URI
	uri = "http://"+bucket+".s3.amazonaws.com/"+object_key
	
	#Create Headers
	headers = {
		'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.localtime()),
		'User-Agent':'S3 Python API'
	}
	headers['Authorization'] = s3_signature.get_auth_header('GET','/'+bucket+'/'+object_key,headers)
	
	#Repeat transaction until successful, or we run out of retries
	retries = 0
	while 1:
		
		c = pycurl.Curl()
		c.setopt(pycurl.URL, uri)
		c.setopt(pycurl.HTTPHEADER, [h+": "+str(headers[h]) for h in headers])
		c.setopt(pycurl.VERBOSE, 0)
		c.setopt(pycurl.HEADER, 0)
		
		#Catch response
		res = cStringIO.StringIO()
		c.setopt(pycurl.WRITEFUNCTION, res.write)
			
		#Do It
		retry = 0
		c.perform()
		
		ret_code = int(c.getinfo(pycurl.RESPONSE_CODE))
		
		if ret_code in [200]: pass #Success
		elif ret_code in [400,403,405,411,412,501]: retry = 1 #We must have messed up the Request (Not Recoverable)
		elif ret_code == 500: retry = 1
		elif ret_code == 503: retry = 1 #Wow,wow...Hold your horses! We probably hit a SlowDown
		else: retry = 1 #Empty response (DNS/Connect timeout perhaps?)
		
		c.close()
		
		if retry == 0: break
		elif retry == 1 and retries <= s3_config.max_retries:
			retries += 1
			print "cURL transaction failed. Retrying...\n"
		
		else:
			print "cURL transaction failed too many times. Giving up...\n"
			return 0
	
	#@TODO
	# Well, implement some parsing, as soon as I figure out what to use
	# this function for... :P
	print res.getvalue()