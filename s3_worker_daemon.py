#!/usr/bin/env python

import s3_daemon
import s3_config
import socket, sys, threading, base64, time


#########################################################################
#########################################################################
# SERVER STUFF

#Thread for accepting instructions
class worker_th(threading.Thread):
	tlist = [] # list of all current accept threads
	maxthreads = s3_config.max_workers # max number of threads we're allowing
	max_files = s3_config.max_files # max number of files we'll request in one go
	
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
	time_started = time.time()

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
				raise Exception("Error while setting up Socket connection to Queue server") #Re-raise exception
			
			
			#Time to pick up some work
			work_list = []
			try:
				
				for i in range(6):
					
					sock.send(base64.b64encode("get|"+str(worker_th.max_files))+"\n")
					ret = base64.b64decode(sock.recv(2048)).strip()
					
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
			
			
			############################
			## UPLOADING HAPPENS HERE
			for work in work_list:
				worker_th.debug_step('Got work: '+work)
				time.sleep(5)
			
		except: pass
		finally:
			#At the end, clean up after ourselves
			self.cleanup()
			return
	

	#Removes this thread from executing list,
	# and (optionally) signals for a new thread to be created
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
		
		

#The daemon itself
class S3WorkerDaemon(s3_daemon.Daemon):
	def run(self):
		
		worker_th.debug_check('Server up and listening')
		
		while True:
			
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


#Check run command
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