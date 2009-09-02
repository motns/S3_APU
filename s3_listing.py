#!/usr/bin/env python
import s3_signature

def list_objects(bucket,prefix="",marker="",maxkeys=10,delimiter=""):

	if bucket.strip() == "": raise "You have to pass in  a bucket name"

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
			p.append(param+"="+params[param])
		uri += "".join(p)
		
		
		
		$req = new HttpRequest($uri);
		$req->setMethod(HTTP_METH_GET);
		
		//Create Headers
		$headers = array(
			'Date'=>gmdate("D, d M Y H:i:s T"),
			'User-Agent'=>"S3 PHP Api");	
		$headers['Authorization'] = $this->get_auth_header('GET',"/".$bucket."/",$headers);
		
		//Add Headers to request
		$req->setHeaders($headers);
		
		//Do it
		$response = $req->send();
		
		//Parse Object List
		$response_xml = new SimpleXMLElement($response->getBody());
		var_dump($response_xml);
		$return = array();
		foreach($response_xml->Contents As $object){
		  $return[] = array(
				"Key"=>(string) $object->Key,
				"LastModified"=>(string) $object->LastModified,
				"ETag"=>(string) $object->ETag,
				"Size"=>(string) $object->Size,
				"Owner"=>array(
					 "ID"=>(string) $object->Owner->ID,
					 "DisplayName"=>(string) $object->Owner->DisplayName
				)
		  );
		}
		
		return $return;
		
	}