#!/usr/bin/env python
import s3_keys
import base64
import hashlib
import hmac

def get_auth_header(verb,path="",dict_headers={}):

	string_to_sign = "";
	headers = dict_headers.copy()
		
	#Add Action (verb)
	string_to_sign += str(verb)+"\n"
		
	#Add Content MD5
	#Only applies to PUT requests, but it's optional
	if 'Content-MD5' in headers:
		string_to_sign += str(headers['Content-MD5'])+"\n"
		del headers['Content-MD5']
	else:
		string_to_sign += "\n"
		
		
	#Add Content type
	#Only applies to PUT
	if 'Content-Type' in headers:
		string_to_sign += str(headers['Content-Type'])+"\n"
		del headers['Content-Type']
		
	else:
		string_to_sign += "\n"
		
		
	#Add date
	string_to_sign += headers['Date']+"\n"
	del headers['Date']
		
		
	#Add the rest of the headers
	# AWS Requires that the headers in the signature be converted
	# to all lowercase, and be sorted alphabetically
	sorted_header_keys = sorted(headers.keys())
	
	for header in sorted_header_keys:
		#Only add Amazon control headers
		if header.lower()[0:5] == "x-amz":
			string_to_sign += header.lower()+":"+headers[header].strip()+"\n"
		
		
		
	#Add path
	#Bucket + Object (without Query string, except for Sub-resources)
	string_to_sign += path
	print string_to_sign
		
	#Generate signature
	digest_encoded = base64.b64encode(
		hmac.new(
			s3_keys.aws_secret_key,string_to_sign.encode("utf-8"),hashlib.sha1
		).digest()
	)
	signature = "AWS "+s3_keys.aws_access_key_id+":"+digest_encoded
		
	return signature