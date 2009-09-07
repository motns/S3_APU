#!/usr/bin/env python
import s3_signature
import pycurl
import time
import cStringIO
from xml.dom import minidom

def list_objects(bucket,prefix="",marker="",maxkeys=10,delimiter=""):

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
	headers['Authorization'] = s3_signature.get_auth_header('GET', '/'+bucket+'/', headers)
	
	
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
		
	#@TODO: Some error handling/parsing here!
	
	#print c.getinfo(pycurl.HTTP_CODE)
	response = res.getvalue().split("\r\n")
	
	#Get out headers and body
	body = response[-1]
	headers = [i for i in response[1:-2] if i != '']
	
	print headers
	
	print "=== Keys:"
	dom = minidom.parseString(body)
	for key in dom.getElementsByTagName('Key'):
		print key.firstChild.nodeValue
	
	print "\n=== Common Prefixes:"	
	for prefixes in dom.getElementsByTagName('CommonPrefixes'):
		print prefixes.getElementsByTagName('Prefix')[0].firstChild.nodeValue
	
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
		
		
		
##############################################################################################
##############################################################################################

def get_object(bucket,object_key=""):

	if bucket.strip() == "": raise "You have to pass in  a bucket name"
	if object_key.strip() == "": raise "You have to pass in an object key"

	#Build Base URI
	uri = "http://"+bucket+".s3.amazonaws.com/"+object_key
	
	#Create Headers
	headers = {
		'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime()),
		'User-Agent':'S3 Python API'
	}
	headers['Authorization'] = s3_signature.get_auth_header('GET','/'+bucket+'/'+object_key,headers)
	
	
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
		
	#@TODO: Some error handling/parsing here!
	
	#print c.getinfo(pycurl.HTTP_CODE)
	#response = res.getvalue().split("\r\n")
	print res.getvalue()
	
	#Get out headers and body
	#body = response[-1]
	#headers = [i for i in response[1:-2] if i != '']
	
	#print headers
	
	#dom = minidom.parseString(body)
	#for content in dom.getElementsByTagName('Contents'):
	#	print content.getElementsByTagName('Key')[0].firstChild.nodeValue