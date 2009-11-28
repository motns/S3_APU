#!/usr/bin/env python
#
# chkconfig: 3 89 88
# description: Multi-threaded cURL-based uploader for S3
# processname: s3_worker_daemon

import s3_daemon
import s3_config
import s3_signature
import pycurl
import cStringIO
import base64, hashlib, os, socket, sys, threading, time


#########################################################################
#########################################################################
# SERVER STUFF

#Thread for accepting instructions
class worker_th(threading.Thread):
	tlist = [] # list of all current worker threads
	maxthreads = s3_config.max_workers # max number of threads we're allowing
	max_files = s3_config.max_files # max number of items we'll request in one go

	th_event = threading.Event() # event to signal OK to create more threads
	list_lock = threading.Lock() # lock to guard tlist

	#Locks for shared logging
	event_log_lock = threading.Lock()
	error_log_lock = threading.Lock()
	debug_log_lock = threading.Lock()

	#Counter variables for total commands executed, and breakdowns
	stat_lock = threading.Lock()
	total_count = 0
	upl_count = 0
	mkd_count = 0
	del_count = 0
	time_started = time.time()

	#Performance stats (still covered by stat_lock)
	# Measured in seconds and microseconds
	# Please Note:
	# The figures may also be affected by the maximum number of files
	# included in each transaction
	slow_trans = 0.0 #The slowest transaction ever
	fast_trans = 0.0 #The fastest transaction ever
	avg_trans = 0.0 #Moving average of transaction times

	#File stats
	# Size in KB
	sml_file_size = 0.0
	lrg_file_size = 0.0
	avg_file_size = 0.0
	total_file_size = 0.0 #Cumulative total of all files uploaded

	#Used for Amazon S3 Backoff mode
	backoff_lock = threading.Lock()
	backoff_set_status = False
	backoff_set_time = time.time()

	def __init__(self,id):
		threading.Thread.__init__(self)
		self.threadnum = id # thread ID

	def run(self):
		try:
			worker_th.debug_step('Thread ID '+str(self.threadnum)+" running")
			
			#Let's hook up with Big Daddy
			try:
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				
				#Attempt to connect to Queue server up to 3 times (30 second intervals)
				for i in range(3):
					try:
						sock.connect((s3_config.queue_server_ip, s3_config.queue_server_port))
						break
					except:
						worker_th.log_error("Error while connecting to Queue server",2)
						if i == 2:
							raise Exception("Failed to establish connection") #Re-raise exception, to trigger cleanup
						else:
							time.sleep(30)
				
				worker_th.debug_step('Thread ID '+str(self.threadnum)+" connected to Queue server")
				
			except Exception:
				raise Exception("Failed to establish connection") #Re-raise exception, to trigger cleanup
			except:
				worker_th.log_error("Error while setting up Socket connection to Queue server",3)
				raise Exception("Error while setting up Socket connection to Queue server") #Re-raise exception for cleanup
			
			
			#Time to pick up some work
			work_list = []
			try:
				for i in range(6):	
					sock.send(base64.b64encode("get|"+str(worker_th.max_files))+"\n")
					# Max receive calibrated for the 30 item limit on Get from Queue
					ret = base64.b64decode(sock.recv(8192)).strip()
					
					#If there's no work to be done at the moment
					if ret == "":
						if i == 5:
							#It's enough for now. Stop this thread and start a new one
							raise Exception("No work to be done")
						else: #Retry in a bit
							time.sleep(10)
						
					else:
						if ret != "":
							if ";" in ret:
								work_list = ret.split(";")
							else:
								work_list.append(ret)
						break
				
			except Exception:
				raise Exception("No work to be done") #Re-raise exception, to trigger cleanup
			except:
				worker_th.log_error("Error while getting and parsing work from Q Server",3)
				raise Exception("Error while getting and parsing work from Q Server") #Re-raise exception
			finally:
				sock.close()
			
			
			###################################################################
			###################################################################
			## S3 TRANSACTIONS HAPPEN HERE
			
			#Sort work based on S3 Destination Object Key
			# Sorted keys are less likely to cause a 503 SlowDown error
			work_list.sort(lambda x,y: cmp(x.split('|')[-1], y.split('|')[-1]))
			
			#Run until all the work is done or we just had enough
			transaction_attempts = 1
			while 1:
				
				multi_curl = pycurl.CurlMulti()
				multi_curl.setopt(pycurl.M_PIPELINING, 1) #Let's pipe calls, rather than running them in parallel
				multi_curl.handles = [] #References to original easy curl handles
				
				#Parse work and create cURL easy handles
				for i in range(len(work_list)):
					
					work = work_list[i]
					
					#####################################################
					#Parse Instruction
					
					if "|" not in work: #Validate generic format
						worker_th.log_error("Malformed instruction received",2)
						continue
					else:
						cmd = work[0:3]
						if cmd not in ("mkd","upl","del"): #Validate instructions
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
						
						#Arbitrary list of extensions I thought I might use in the future
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
						
						#Check if file exists
						# Retry 3 times if missing - To get around race conditions
						for r in range(3):
							if os.path.exists(source_path) == False:
								if r >= 2:
									raise Exception("The file specified doesn't exist")
								else:
									time.sleep(5)
							else: break
						
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
							'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.localtime()),
							'User-Agent':'S3 APU',
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
							'Date':time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.localtime()),
							'User-Agent':'S3 APU'
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
					#c.setopt(pycurl.TIMEOUT, 30) #Disabled timeout, to get around Python being crashed by Timeout SIGNAL
					
					#Store original instruction in handle
					# in case it needs to be repeated
					c.instruction = work
					
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
					
					#Push onto stack
					multi_curl.add_handle(c)
					multi_curl.handles.append(c)
					
					#Update file stats
					# only on first run
					if transaction_attempts == 1:
						try:
							worker_th.stat_lock.acquire()
							
							worker_th.total_count += 1
							
							if instruction == "mkd":
								worker_th.mkd_count += 1
							elif instruction == "upl":
								worker_th.upl_count += 1
								
								#Convert to KB for logging
								log_file_size = filesize / 1024
								
								worker_th.sml_file_size = min(log_file_size,worker_th.sml_file_size) if worker_th.sml_file_size > 0.00 else log_file_size
								worker_th.lrg_file_size = max(log_file_size,worker_th.lrg_file_size)
								worker_th.avg_file_size = ((worker_th.avg_file_size + log_file_size) / 2) if worker_th.avg_file_size > 0.00 else log_file_size
								worker_th.total_file_size += log_file_size
								
							elif instruction == "del":
								worker_th.del_count += 1
							
						finally:
							worker_th.stat_lock.release()
				
				#Check if there's anything to do at all
				if len(multi_curl.handles) == 0:
					break
				
				#Check if we have to wait because of a SlowDown in effect
				try:
					worker_th.backoff_lock.acquire()
					
					if worker_th.backoff_set_status == True:
						if (time.time() - worker_th.backoff_set_time) >= s3_config.slowdown_time:
							worker_th.backoff_set_status = False
						else:
							time.sleep(3)
					
				finally:
					worker_th.backoff_lock.release()
				
				
				###############################################################################
				## Run transactions
				
				worker_th.debug_step("Running transactions")
				
				start_time = time.time()
				
				#Perform requests
				num_handles = len(multi_curl.handles)
				while num_handles:
					while 1:
						ret, num_handles = multi_curl.perform()
						if ret != pycurl.E_CALL_MULTI_PERFORM: #Check if we have to run perform again immediately
							break
				
				#Check the results
				work_for_retry = [] #List of instructions to try again
				for handle in multi_curl.handles:
					
					#Whether to remove this item from the stack
					to_remove = False
					
					#Get response code
					ret_code = int(handle.getinfo(pycurl.RESPONSE_CODE))
					
					if ret_code in [200,204]: #Success (204 for Deletes, 200 for rest)
						if ret_code == 200:
							worker_th.log_event("Successfully uploaded object: %s" % handle.getinfo(pycurl.EFFECTIVE_URL))
						elif ret_code == 204:
							worker_th.log_event("Successfully deleted object: %s" % handle.getinfo(pycurl.EFFECTIVE_URL))
						
						to_remove = True
						
					elif ret_code in [400,403,405,411,412,501]: #We must have messed up the Request (Not Recoverable)
						#@TODO: Maybe try to repair/re-build request based on response received
						worker_th.log_error("Transaction failed with code %d for object: %s" % (ret_code, handle.getinfo(pycurl.EFFECTIVE_URL)),3)
						
						to_remove = True
						
					elif ret_code == 500:
						
						if transaction_attempts >= 3:
							worker_th.log_error("Transaction failed with code 500. Giving up on object: %s" % handle.getinfo(pycurl.EFFECTIVE_URL),3)
							to_remove = True
						else:
							worker_th.log_error("Transaction failed with code 500. Try again.",2)
						
					elif ret_code == 503: #Wow,wow...Hold your horses! We probably hit a SlowDown
						worker_th.log_error("Received 503 from server. Initiating SlowDown and retrying",2)
						
						try:
							worker_th.backoff_lock.acquire()
							
							if worker_th.backoff_set_status == False: #Don't reset Backoff Mode
								worker_th.backoff_set_status = True
								worker_th.backoff_set_time = time.time()
							
						finally:
							worker_th.backoff_lock.release()
						
					else: #Empty response (DNS/Connect timeout perhaps?)
						if transaction_attempts >= 3:
							worker_th.log_error("cURL transaction failed. Giving up on object: %s" % handle.getinfo(pycurl.EFFECTIVE_URL),3)
							to_remove = True
						else:
							worker_th.log_error("cURL transaction failed. Trying again.",2)
						
						
					#Should we retry this transaction?
					if to_remove == False:
						work_for_retry.append(handle.instruction)
					
					#Remove cURL handle from stack and terminate
					try:
						multi_curl.remove_handle(handle)
						handle.close()
					except: pass
				
				
				#Update transaction stats
				try:
					worker_th.stat_lock.acquire()
					transaction_time = time.time() - start_time
					
					worker_th.slow_trans = max(worker_th.slow_trans,transaction_time)
					worker_th.fast_trans = min(worker_th.fast_trans,transaction_time) if worker_th.fast_trans > 0.00 else transaction_time
					worker_th.avg_trans = ((worker_th.avg_trans + transaction_time) / 2) if worker_th.avg_trans > 0.00 else transaction_time
					
				finally:
					worker_th.stat_lock.release()
				
				
				#Close current cURL Multi stack (we'll create a new one later)
				# This is to prevent timeouts
				try:
					multi_curl.close()
					del(multi_curl)
				except: pass
				
				
				#If there are no unfinished cURL responses left
				if len(work_for_retry) == 0: #Halleluja, we're done!
					break
				elif transaction_attempts >= 3: #That's enough... :(
					worker_th.log_error("MultiCurl transactions failed 3 times. Giving up...",3)
					break
				else:
					transaction_attempts += 1
					
					#Re-build work list with outstanding items
					# Also re-sort, just in case
					work_list = work_for_retry
					work_list.sort(lambda x,y: cmp(x.split('|')[-1], y.split('|')[-1]))
					
					#Let things cool down a bit
					time.sleep(3)
			
			worker_th.output_stats()
			
			
		except: pass #Lets catch all exceptions for now (@TODO: Unexpected Error logging here)
		finally:
			#At the end, clean up after ourselves
			self.cleanup()
			return


	#Removes this thread from executing list,
	# and (if required) signals for a new thread to be created
	def cleanup(self):
		try:
			#Take ourselves off the thread list
			worker_th.list_lock.acquire()
			worker_th.tlist.remove(self)

			#Do we need to call for a new thread?
			if len(worker_th.tlist) == worker_th.maxthreads - 1:
				worker_th.th_event.set()
				worker_th.th_event.clear()

		except:
			worker_th.log_error("Error while finishing up thread execution (Perhaps problems with Event firing?)",3)
		finally:
			worker_th.list_lock.release()


	@staticmethod
	def newthread():
		worker_th.debug_check('Creating new thread')

		worker_th.list_lock.acquire()
		t = worker_th(len(worker_th.tlist))
		worker_th.tlist.append(t)
		worker_th.list_lock.release()
		t.start()


	#############################################################
	## Logging

	#Log debug messages (if debugging is on)
	@staticmethod
	def log_debug(msg,level=1):
		if s3_config.debug_level < level: return

		if level == 1:
			type = 'CHECKPOINT'
		else:
			type = 'STEP'

		worker_th.debug_log_lock.acquire()
		log = "["+time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime())+"] - "+type+": "+msg+"\n"
		try:
			f = file(s3_config.log_folder+'/s3_worker_debug.log','a+')
			try:
				f.write(log)
			finally:
				f.close()
		except: pass
		worker_th.debug_log_lock.release()

	#Debug log aliases
	@staticmethod
	def debug_check(msg):
		worker_th.log_debug(msg,1)

	@staticmethod
	def debug_step(msg):
		worker_th.log_debug(msg,2)


	#Log generic event to log
	@staticmethod
	def log_event(msg):
		worker_th.event_log_lock.acquire()
		log = "["+time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime())+"]: "+msg+"\n"
		try:
			f = file(s3_config.log_folder+'/s3_worker_event.log','a+')
			try:
				f.write(log)
			finally:
				f.close()
		except: pass
		worker_th.event_log_lock.release()


	#Log an error of a certain level (severity)
	# level 1: Notice, or unexpected behaviour (non-fatal, recoverable)
	# level 2: Warning. (non-fatal, recoverable)
	# level 3: Error. (Fatal, non-recoverable)
	@staticmethod
	def log_error(msg,level=1):
		worker_th.error_log_lock.acquire()

		if level == 1:
			type = 'NOTICE'
		elif level == 2:
			type = 'WARNING'
		else:
			type = 'ERROR'

		log = "["+time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime())+"] - "+type+": "+msg+"\n"
		try:
			f = file(s3_config.log_folder+'/s3_worker_error.log','a+')
			try:
				f.write(log)
			finally:
				f.close()
		except: pass
		worker_th.error_log_lock.release()
		
		
	@staticmethod
	def output_stats():
		
		ret = ""
		
		#Get stat counter values
		try:
			worker_th.stat_lock.acquire()
			
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
			ret += "|total_file_size:%F" % worker_th.total_file_size
			ret += "|backoff_status:%s" % worker_th.backoff_set_status
			
			try:
				f = file(s3_config.log_folder+'/s3_worker_performance','w+')
				try:
					f.write(ret)
				finally:
					f.close()
			except: pass
			
		finally:
			worker_th.stat_lock.release()
			pass



#The daemon itself
class S3WorkerDaemon(s3_daemon.Daemon):
	def run(self):
		worker_th.debug_check('Server up and listening')
		
		while 1:
			try:
				try:
					worker_th.list_lock.acquire()
					if len(worker_th.tlist) >= worker_th.maxthreads: #We're maxed out
						worker_th.list_lock.release()
						worker_th.th_event.wait() #Wait for a thread to finish
					else:
						worker_th.list_lock.release()
					
				except:
					worker_th.log_error('Error while testing available threads',3)
					try:
						worker_th.list_lock.release()
					except: pass
					raise Exception("List lock error")
				
				worker_th.newthread()
			except:
				worker_th.log_error('Error while setting up new thread',3)


#########################################################################
#########################################################################
# DAEMON STUFF

if __name__ == "__main__":
	daemon = S3WorkerDaemon('/tmp/s3_worker_daemon.pid')
	if len(sys.argv) == 2:
		if 'start' == sys.argv[1]:
				daemon.start()
		elif 'stop' == sys.argv[1]:
				daemon.stop()
		elif 'restart' == sys.argv[1]:
				daemon.restart()
		else:
				print "Unknown command"
				sys.exit(2)
		sys.exit(0)
	else:
		print "usage: %s start|stop|restart" % sys.argv[0]
		sys.exit(2)