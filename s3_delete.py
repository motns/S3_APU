#!/usr/bin/env python
import s3_signature
import pycurl
import time
import cStringIO
from xml.dom import minidom

def delete_object(bucket,object_key=""):

	if bucket.strip() == "": raise "You have to pass in  a bucket name"
	if object_key.strip() == "": raise "You have to pass in an object key"

	#Build Base URI
	uri = "http://"+bucket+".s3.amazonaws.com/"+object_key
	
	#Create Headers
	headers = {
		'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime()),
		'User-Agent':'S3 Python API'
	}
	headers['Authorization'] = s3_signature.get_auth_header('DELETE','/'+bucket+'/'+object_key,headers)
	#print headers
	
	
	#Initiate curl object
	c = pycurl.Curl()
	c.setopt(pycurl.URL, uri)
	c.setopt(pycurl.CUSTOMREQUEST, "DELETE")
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