#!/usr/bin/env python
import s3_config
import s3_signature
import base64
import pycurl
import time
import os
import cStringIO
import hashlib

#Dummy class for testing
class worker_th:

	#Counter variables for total commands executed, and breakdowns
	#stat_lock = threading.Lock()
	total_count = 0
	upl_count = 0
	mkd_count = 0
	del_count = 0
	time_started = time.time()

	#Performance stats (still covered by stat_lock)
	#  Measured in seconds and microseconds
	slow_trans = 0.0 #The slowest transaction ever
	fast_trans = 0.0 #The fastest transaction ever
	avg_trans = 0.0 #Moving average of transaction times

	#File stats
	#  The size in KB
	sml_file_size = 0.0
	lrg_file_size = 0.0
	avg_file_size = 0.0


	#Used for Amazon S3 Backoff mode
	#backoff_lock = threading.Lock()
	backoff_set_status = False
	backoff_set_time = time.time()


	@staticmethod
	def log_error(msg,level):
		print msg
	
	
	@staticmethod
	def log_event(msg):
		print msg

	@staticmethod
	def output_stats():
		
		ret += ""
		
		#Get stat counter values
		try:
			#worker_th.stat_lock.acquire()
			
			ret += "total_cmd:%d" % worker_th.total_count
			ret += "|mkd_cmd:%d" % worker_th.mkd_count
			ret += "|upl_cmd:%d" % worker_th.upl_count
			ret += "|del_cmd:%d" % worker_th.del_count
			ret += "|uptime:%d" % (time.time() - worker_th.time_started)
			ret += "|slow_trans:%F" % worker_th.slow_trans
			ret += "|fast_trans:%F" % worker_th.fast_trans
			ret += "|avg_trans:%F" % worker_th.avg_trans
			ret += "|sml_file_size:%F" % worker_th.sml_file_size
			ret += "|lrg_file_size:%F" % worker_th.lrg_file_size
			ret += "|avg_file_size:%F" % worker_th.avg_file_size
			ret += "|backoff_status:%s" % worker_th.backoff_set_status
			
		finally:
			#worker_th.stat_lock.release()
			pass
		
		print ret


#work_list = [
#	"mkd|testdir1",
#	"mkd|testdir1/testdir2",
#	"mkd|testdir1/testdir2/testdir3",
#	"upl|/Users/adamb/Sites/dummy.txt|testdir1/testdir2/testdir3/dummy.txt",
#	"del|dummy.txt"
#]

work_list = [
	"mkd|test_images",
	"mkd|test_images/images1",
	"mkd|test_images/images2",
	"upl|/Users/adamb/Pictures/P1030596.JPG|test_images/images1/P1030596.JPG",
	"upl|/Users/adamb/Pictures/P1040818.JPG|test_images/images1/P1040818.JPG",
	"upl|/Users/adamb/Pictures/ovi_csoport.jpg|test_images/images2/ovi_csoport.jpg",
	"upl|/Users/adamb/Pictures/profile_cool.jpg|test_images/images2/profile_cool.jpg",
	"upl|/Users/adamb/Pictures/profile_prof.jpg|test_images/images2/profile_prof.jpg"
]

#work_list.append("upl|/Users/adamb/Pictures/P1040139.JPG|test_images/images1/P1040139.JPG")

#Sort work based on S3 Destination Object Key
# Sorted keys are less likely to cause a 503 SlowDown error
work_list.sort(lambda x,y: cmp(x.split('|')[-1], y.split('|')[-1]))

######################################################################

#Initiate Multi Object
multi_curl = pycurl.CurlMulti()
multi_curl.setopt(pycurl.M_PIPELINING, 1) #Let's pipe calls, rather than running them in parallel
multi_curl.handles = [] #References to original easy curl handles

#List of cStringIO objects, with responses
response_list = []

for i in range(len(work_list[0:3])):

	work = work_list[i]
	print work

	#####################################################
	#Parse Instruction

	if "|" not in work: #Validate generic format
		worker_th.log_error("Malformed instruction received",2)
		continue
	else:
		cmd = work[0:3]
		if cmd not in ("mkd","upl","del"): #Validate instruction
			worker_th.log_error("Invalid instruction received",2)
			continue
		else:
			params = work.split("|")

			#Validate parameter lengths
			if (cmd in ("mkd","del") and len(params) != 2) or (cmd in ("upl") and len(params) != 3):
				worker_th.log_error("Invalid argument length",2)
				continue
			else:
				#Ok, we have a valid instruction
				# Let's parse it into work
				instruction = params[0]
				if instruction == "mkd" or instruction == "del":
					source_path = ""
					destination_key = params[1]
				elif instruction == "upl":
					source_path = params[1]
					destination_key = params[2]
				else:
					worker_th.log_error("Error while parsing instruction",2)
					continue


	#####################################################
	# Build Request headers, and create AWS signature

	#Set content type, and check file
	if instruction == "upl":

		#Basic Content Type guessing, based on file extension
		file_parts = source_path.split(".")
		if len(file_parts) > 1:
			file_ext = file_parts[-1].lower()
		else: file_ext = ""

		#Arbitrary list of extension I thought I might use in the future
		if file_ext in ["jpeg","jpg","jpe"]:
			content_type = "image/jpeg"
		elif file_ext in ["gif"]:
			content_type = "image/gif"
		elif file_ext in ["png"]:
			content_type = "image/x-png"
		elif file_ext in ["tiff","tif"]:
			content_type = "image/tiff"
		elif file_ext in ["html","htm"]:
			content_type = "text/html"
		elif file_ext in ["css"]:
			content_type = "text/css"
		elif file_ext in ["js"]:
			content_type = "text/javascript"
		elif file_ext in ["pdf"]:
			content_type = "application/pdf"
		elif file_ext in ["rtf"]:
			content_type = "application/rtf"

		else:
			content_type = "text/plain"

		#Linux file system mode for FILE(-), with 775
		meta_mode = int(0100775)

		if os.path.exists(source_path) == False: raise Exception("The file specified doesn't exist")

		#Get file checksum
		checksum = base64.b64encode(
			hashlib.md5(
				open(source_path, 'rb').read()
			).digest()
		)

	elif instruction == "mkd":
		content_type = "application/x-directory"

		#Linux file system mode for DIRECTORY(d), with 775
		meta_mode = int(040775)

		checksum = ""
	else: #It's a 'del' instruction
		pass

	#Build Base URI
	uri = "http://"+s3_config.s3_bucket+".s3.amazonaws.com/"+destination_key

	#Create Headers
	if instruction == "upl" or instruction == "mkd":
		headers = {
			'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime()),
			'User-Agent':'S3 PTS',
			'Content-Type':content_type,
			'x-amz-acl':'public-read',
			'x-amz-meta-gid': str(s3_config.upload_gid),
			'x-amz-meta-mode': str(meta_mode),
			'x-amz-meta-mtime': str(int(time.time())),
			'x-amz-meta-uid': str(s3_config.upload_uid)
		}
		
		if checksum != "": headers['Content-MD5'] = checksum
		
		headers['Authorization'] = s3_signature.get_auth_header('PUT', '/'+s3_config.s3_bucket+'/'+destination_key, headers)

	elif instruction == "del":
		headers = {
			'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime()),
			'User-Agent':'S3 PTS'
		}
		
		headers['Authorization'] = s3_signature.get_auth_header('DELETE', '/'+s3_config.s3_bucket+'/'+destination_key, headers)


	###########################################################
	# Initiate cURL object, then put it on MultiCurl stack

	c = pycurl.Curl()
	c.setopt(pycurl.URL, uri)
	c.setopt(pycurl.HTTPHEADER, [h+": "+str(headers[h]) for h in headers])
	c.setopt(pycurl.VERBOSE, 0)
	c.setopt(pycurl.FOLLOWLOCATION, 1) #Follow 307's returned by S3
	c.setopt(pycurl.MAXREDIRS, 3) #Let's not go crazy!
	#c.setopt(pycurl.HEADER, 1)
	#c.setopt(pycurl.TIMEOUT, 5)


	if instruction == "upl":

		c.setopt(pycurl.UPLOAD, 1)

		#Read file for upload
		c.setopt(pycurl.READFUNCTION, open(source_path, 'rb').read)

		# Set size of file to be uploaded.
		filesize = os.path.getsize(source_path)
		c.setopt(pycurl.INFILESIZE, filesize)

	elif instruction == "mkd":

		c.setopt(pycurl.UPLOAD, 1)

		#Fake empty file object
		fake_file = cStringIO.StringIO()

		#Read file for upload
		c.setopt(pycurl.READFUNCTION, fake_file.read)

		# Set size of file to be uploaded.
		c.setopt(pycurl.INFILESIZE, 0)

	elif instruction == "del":
		c.setopt(pycurl.CUSTOMREQUEST, "DELETE")

	#Catch response
	#response_list.append(cStringIO.StringIO())
	#c.setopt(pycurl.WRITEFUNCTION, response_list[i].write)

	#Push onto stack
	multi_curl.add_handle(c)
	multi_curl.handles.append(c)


#Check if we have to wait because of a SlowDown in effect
try:
	#worker_th.backoff_lock.acquire()
	
	if worker_th.backoff_set_status == True:
		
		#Check if time elapsed was enough
		if (time.time() - worker_th.backoff_set_time) >= s3_config.slowdown_time:
			worker_th.backoff_set_status = False #Disable SlowDown mode
		else:
			print "Backoff mode"
			time.sleep(3) #Chill out for a second
	
finally:
	#worker_th.backoff_lock.release()
	pass

#Run transactions until they're all done, or there's a fatal error
retries = 0
start_time = time.time()
while True:
	
	#Perform requests
	num_handles = len(multi_curl.handles)
	if num_handles > 0:
		while num_handles:
			while 1:
				ret, num_handles = multi_curl.perform()
				if ret != pycurl.E_CALL_MULTI_PERFORM: #Check if we have to run perform again immediately
					break
	

	#Check the results
	for handle in multi_curl.handles:
		
		#Wait for response code
		while 1:
			try:
				print "Getting response code"
				ret_code = handle.getinfo(pycurl.RESPONSE_CODE)
				print "Code: %d" % ret_code
				break
			except:
				print "Exception"
		
		if ret_code in [200,204]: #Success (204 for Deletes, 200 for rest)
			if ret_code == 200:
				worker_th.log_event("Successfully uploaded object: %s" % handle.getinfo(pycurl.EFFECTIVE_URL))
			elif ret_code == 204:
				worker_th.log_event("Successfully deleted object: %s" % handle.getinfo(pycurl.EFFECTIVE_URL))
			
			multi_curl.remove_handle(handle)
			multi_curl.handles.remove(handle)
			handle.close()
			
		elif ret_code in [400,403,405,411,412,501]: #We must have messed up the Request (Not Recoverable)
			#@TODO: Maybe try to repair/re-build request based on response received
			worker_th.log_error("Transaction failed with code %d for object: %s" % (ret_code, handle.getinfo(pycurl.EFFECTIVE_URL)),3)
			
			multi_curl.remove_handle(handle)
			multi_curl.handles.remove(handle)
			handle.close()
			
		elif ret_code == 500:
			worker_th.log_error("Transaction failed with code 500. Try again.",2)
			
			if retries >= 3:
				worker_th.log_error("cURL transaction failed. Giving up...",3)
				multi_curl.remove_handle(handle)
				multi_curl.handles.remove(handle)
				handle.close()
			else:
				worker_th.log_error("cURL transaction failed. Try again.",2)
				retries += 1
			
		elif ret_code == 503: #Wow,wow...Hold your horses! We probably hit a SlowDown
			worker_th.log_error("Received 503 from server. Initiating SlowDown and retrying",2)
			
			try:
				#worker_th.backoff_lock.acquire()
				
				if backoff_set_status == False: #Don't reset Backoff Mode
					backoff_set_status = True
					backoff_set_time = time.time()
				
			finally:
				#worker_th.backoff_lock.release()
				pass
			
		else: #Empty response (DNS/Connect timeout perhaps?)
			print "Empty respose!"
			if retries >= 3:
				worker_th.log_error("cURL transaction failed. Giving up...",3)
				multi_curl.remove_handle(handle)
				multi_curl.handles.remove(handle)
				handle.close()
			else:
				worker_th.log_error("cURL transaction failed. Trying again.",2)
				retries += 1

	
	#If there are no unfinished cURL responses left
	if len(multi_curl.handles) == 0:
		break
	else: #Let things cool down a bit
		print "In Retry loop"
		#time.sleep(3)
	

transaction_time = time.time() - start_time
print "Transaction took: %F" % transaction_time