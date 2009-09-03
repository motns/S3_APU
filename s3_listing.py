#!/usr/bin/env python
import s3_signature
import pycurl
import time
import cStringIO
from xml.dom import minidom

def list_objects(aws_access_key_id,aws_secret_key,bucket,prefix="",marker="",maxkeys=10,delimiter=""):

	if bucket.strip() == "": raise "You have to pass in  a bucket name"

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
		'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime()),
		'User-Agent':'S3 Python API'
	}
	headers['Authorization'] = s3_signature.get_auth_header(aws_access_key_id,aws_secret_key,'GET','/'+bucket+'/',headers)
	
	
	#Initiate curl object
	c = pycurl.Curl()
	c.setopt(pycurl.URL, uri)
	c.setopt(pycurl.HTTPHEADER, [h+": "+str(headers[h]) for h in headers])
	c.setopt(pycurl.VERBOSE, 0)
	
	#Catch response
	res = cStringIO.StringIO()
	c.setopt(pycurl.WRITEFUNCTION, res.write)
		
	#Do It
	c.perform()
	c.close()
		
	#@TODO: Some error handling/parsing here!
	
	#print c.getinfo(pycurl.HTTP_CODE)
	
	dom = minidom.parseString(res.getvalue())
	for content in dom.getElementsByTagName('Contents'):
		print content.getElementsByTagName('Key')[0].firstChild.nodeValue
		
		#foreach($response_xml->Contents As $object){
		#  $return[] = array(
		#		"Key"=>(string) $object->Key,
		#		"LastModified"=>(string) $object->LastModified,
		#		"ETag"=>(string) $object->ETag,
		#		"Size"=>(string) $object->Size,
		#		"Owner"=>array(
		#			 "ID"=>(string) $object->Owner->ID,
		#			 "DisplayName"=>(string) $object->Owner->DisplayName
		#		)
		#  );
		#}
		