#!/usr/bin/env python
from ..conf import s3_config
import s3_signature
import pycurl
import time, cStringIO
from xml.dom import minidom

def list_objects(bucket,prefix="",marker="",maxkeys=100,delimiter=""):

	if bucket.strip() == "": raise "You have to pass in a bucket name"

	#Check for params
	params = {}
	if(prefix.strip() != ""): params['prefix'] = prefix
	if(marker.strip() != ""): params['marker'] = marker
	
	full_list = 0 #Whether to get all keys with prefix
	try:
		params['max-keys'] = int(maxkeys)
		if(params['max-keys'] == 0):
			params['max-keys'] = 1000
			full_list = 1
	except: params['max-keys'] = 10
	
	if(delimiter.strip() != ""): params['delimiter'] = delimiter

	#Keep running until we get all the keys requested
	return_keys = []
	while 1:
		
		#Build Base URI
		uri = "http://"+bucket+".s3.amazonaws.com"
		
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
			elif ret_code in [400,403,404,405,411,412,501]: retry = 0 #We must have messed up the Request
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
		
		
		dom = minidom.parseString(res.getvalue())
		for key in dom.getElementsByTagName('Key'):
			return_keys.append(key.firstChild.nodeValue)
		
		#Added for compatibility
		# Will list "folder placeholder" keys, if they're only present as a prefix
		commonPrefixes = dom.getElementsByTagName('CommonPrefixes')
		if len(commonPrefixes) != 0:
			for commonPrefix in commonPrefixes:
				#HACK!
				# This will get the 'Prefix' tag, since it's the only child element
				# Might have to revise this if the S3 implementation changes
				prefix = commonPrefix.firstChild.firstChild.nodeValue[:-1]
				if prefix not in return_keys:
					return_keys.append(prefix)
			
		#Do we need to get more keys?
		if full_list == 1 or len(return_keys) < maxkeys:
			#Is there more to get?
			if len(dom.getElementsByTagName('IsTruncated')) != 0 and \
			dom.getElementsByTagName('IsTruncated')[0].firstChild.nodeValue == 'true':
				
				params['marker'] = dom.getElementsByTagName('NextMarker')[0].firstChild.nodeValue
				continue
		
		break
	
	return return_keys
	
	
################################################################################
################################################################################

# Get object headers, but not the object body
def get_object_meta(bucket,object_key=""):

	if bucket.strip() == "": raise "You have to pass in  a bucket name"
	if object_key.strip() == "": raise "You have to pass in an object key"

	#Build Base URI
	uri = "http://"+bucket+".s3.amazonaws.com/"+object_key
	
	#Create Headers
	headers = {
		'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.localtime()),
		'User-Agent':'S3 Python API'
	}
	headers['Authorization'] = s3_signature.get_auth_header('HEAD','/'+bucket+'/'+object_key,headers)
	
	#Repeat transaction until successful, or we run out of retries
	retries = 0
	while 1:
		
		c = pycurl.Curl()
		c.setopt(pycurl.URL, uri)
		c.setopt(pycurl.HTTPHEADER, [h+": "+str(headers[h]) for h in headers])
		c.setopt(pycurl.VERBOSE, 0)
		c.setopt(pycurl.HEADER, 1)
		c.setopt(pycurl.CUSTOMREQUEST, 'HEAD')
		c.setopt(pycurl.NOBODY, 1)
		
		#Catch response
		res = cStringIO.StringIO()
		c.setopt(pycurl.WRITEFUNCTION, res.write)
			
		#Do It
		retry = 0
		c.perform()
		
		ret_code = int(c.getinfo(pycurl.RESPONSE_CODE))
		
		if ret_code in [200]: pass #Success
		elif ret_code in [400,403,404,405,411,412,501]: retry = 0 #We must have messed up the Request (Not Recoverable)
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
	
	#If successful, parse and return header info
	if ret_code == 200:
		out = {}
		pieces = [line.split(":") for line in res.getvalue().split("\r\n")]
		for piece in pieces[1:]:
			if piece[0] != '':
				out[piece[0]] = ":".join(piece[1:])
		
		return out
		
	else:
		return 0