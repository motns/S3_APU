#!/usr/bin/env python
#
# chkconfig: 3 87 86
# description: A high-speed multi-threaded queueing system, supporting the S3 APU application
# processname: s3_queue_daemon

from s3_apu.lib import s3_daemon
from s3_apu.conf import s3_config
import base64, re, time, socket, sys, threading, Queue

#########################################################################
#########################################################################
# SERVER STUFF

#Thread for accepting instructions
class q_accept_th(threading.Thread):
	
	tlist = [] # list of all current active threads
	maxthreads = 100 # max number of threads we're allowing
	
	th_event = threading.Event() # event to signal OK to create more threads
	list_lock = threading.Lock() # lock to guard tlist
	
	#Locks for shared logging
	event_log_lock = threading.Lock()
	error_log_lock = threading.Lock()
	debug_log_lock = threading.Lock()
	
	#Counter variables for total commands executed, and breakdowns
	stat_lock = threading.Lock()
	total_count = 0
	mkd_count = 0
	upl_count = 0
	cpo_count = 0
	del_count = 0
	get_count = 0
	time_started = time.time()

	queue = Queue.Queue()
	
	def __init__(self,client,id):
		threading.Thread.__init__(self)
		self.threadnum = id # thread ID
		self.client = client
	
	
	def run(self):
		q_accept_th.debug_step('Thread ID '+str(self.threadnum)+" running")
		
		try:
			#While we have instructions coming
			while True:
				#Get and decode message
				msg = base64.b64decode(self.client.recv(1024)).strip()
				
				q_accept_th.debug_step('Got message: '+msg)
				
				#Validate instruction received
				#
				# mkd - create folder in S3 (mkd|path/to/dir)
				# upl - upload file into S3 (upl|/path/to/file|path/in/s3)
				# cpo - copy object within S3 (cpo|/bucket/source/object|destination/path/object)
				# del - delete file from S3 (del|/path/in/s3)
				# get - pop out and return the last item from the queue
				# inf - return information on the current status of the Queue server
				cmd = msg[0:3]
				if cmd == 'mkd':
					try:
						
						#Validate format
						# Also checks for leading '/'
						match = re.match('mkd\|([a-zA-Z0-9\-\_\.\~\/]+)$', msg)
						if(match != None and match.group(1)[0] != '/'):
							
							#Increment stat counters
							try:
								q_accept_th.stat_lock.acquire()
								q_accept_th.total_count += 1
								q_accept_th.mkd_count += 1
							finally:
								q_accept_th.stat_lock.release()
							
							#Put in the Queue
							q_accept_th.queue.put_nowait(msg.split("|"))
							
							#Send confirm
							self.client.send(base64.b64encode('ok')+"\n")
							
						else:
							q_accept_th.log_error('Invalid instruction format: '+msg.strip().replace('\n',' '),1)
							self.client.send(base64.b64encode('invalid')+"\n")
						
					except:
						q_accept_th.log_error("'mkd' instruction failed: "+msg.strip().replace('\n',' '),2)
						self.client.send(base64.b64encode('fail')+"\n")
					
				elif cmd == 'upl':
					try:
						
						#Validate format
						# Also checks for leading '/' in object path
						match = re.match('upl\|([a-zA-Z0-9\-\_\.\~\/]+)\|([a-zA-Z0-9\-\_\.\~\/]+)$', msg)
						if(match != None and match.group(2)[0] != '/'):
							
							#Increment stat counters
							try:
								q_accept_th.stat_lock.acquire()
								q_accept_th.total_count += 1
								q_accept_th.upl_count += 1
							finally:
								q_accept_th.stat_lock.release()
							
							#Put in the Queue 
							q_accept_th.queue.put_nowait(msg.split("|"))
							
							#Send confirm
							self.client.send(base64.b64encode('ok')+"\n")
							
						else:
							q_accept_th.log_error('Invalid instruction format: '+msg.strip().replace('\n',' '),1)
							self.client.send(base64.b64encode('invalid')+"\n")
						
					except:
						q_accept_th.log_error("'upl' instruction failed: "+msg.strip().replace('\n',' '),2)
						self.client.send(base64.b64encode('fail')+"\n")
					
				elif cmd == 'cpo':
					try:
						
						#Validate format
						# Also checks for leading '/' in destination object path
						match = re.match('cpo\|([a-zA-Z0-9\-\_\.\~\/]+)\|([a-zA-Z0-9\-\_\.\~\/]+)$', msg)
						if(match != None and match.group(2)[0] != '/'):
							
							#Increment stat counters
							try:
								q_accept_th.stat_lock.acquire()
								q_accept_th.total_count += 1
								q_accept_th.cpo_count += 1
							finally:
								q_accept_th.stat_lock.release()
							
							#Put in the Queue 
							q_accept_th.queue.put_nowait(msg.split("|"))
							
							#Send confirm
							self.client.send(base64.b64encode('ok')+"\n")
							
						else:
							q_accept_th.log_error('Invalid instruction format: '+msg.strip().replace('\n',' '),1)
							self.client.send(base64.b64encode('invalid')+"\n")
						
					except:
						q_accept_th.log_error("'cpo' instruction failed: "+msg.strip().replace('\n',' '),2)
						self.client.send(base64.b64encode('fail')+"\n")
						
				elif cmd == 'del':
					try:
						
						#Validate format
						# Also checks for leading '/'
						match = re.match('del\|([a-zA-Z0-9\-\_\.\~\/]+)$', msg)
						if(match != None and match.group(1)[0] != '/'):
							
							#Increment stat counters
							try:
								q_accept_th.stat_lock.acquire()
								q_accept_th.total_count += 1
								q_accept_th.del_count += 1
							finally:
								q_accept_th.stat_lock.release()
							
							#Put in the Queue
							q_accept_th.queue.put_nowait(msg.split("|")[0:2])
							
							#Send confirm
							self.client.send(base64.b64encode('ok')+"\n")
							
						else:
							q_accept_th.log_error('Invalid instruction format: '+msg.strip().replace('\n',' '),1)
							self.client.send(base64.b64encode('invalid')+"\n")
						
					except:
						q_accept_th.log_error("'del' instruction failed: "+msg.strip().replace('\n',' '),2)
						self.client.send(base64.b64encode('fail')+"\n")
						
				elif cmd == 'get':
					
					#Validate format
					match = re.match('get\|([0-9]+)$', msg)
					if(match != None):
						
						#Check if we have multiple items to retrieve
						params = msg.split("|")
						try:
							loop = int(params[1] if len(params) == 2 else 1)
						except:
							loop = 1
						
						#Let's not go crazy!
						if loop > 30: loop = 30
						
						#Try to get the requested number of items
						item_list = []
						for i in range(loop):
							try:
								#Increment stat counters
								try:
									q_accept_th.stat_lock.acquire()
									q_accept_th.total_count += 1
									q_accept_th.get_count += 1
								finally:
									q_accept_th.stat_lock.release()
									
								#Only do timeouts if it's the first iteration
								#  We don't want to waste time when we already have
								#  something in our list to work with
								if i == 0:
									item = q_accept_th.queue.get(True, 10)
								else:
									item = q_accept_th.queue.get_nowait()
								
								if item != "": item_list.append('|'.join(item))
								
							except Queue.Empty: pass
							except:
								q_accept_th.log_error("'get' instruction failed: "+msg.strip().replace('\n',' '),2)
						
						#Send back the joined list
						if len(item_list) > 0:
							self.client.send(base64.b64encode(";".join(item_list))+"\n")
						else:
							self.client.send(base64.b64encode("")+"\n")
						
					else:
						q_accept_th.log_error('Invalid instruction format: '+msg.strip().replace('\n',' '),1)
						self.client.send(base64.b64encode('invalid')+"\n")
					
				elif cmd == 'inf':
					ret = ''
					
					#Get (approximate) size of queue
					try:
						ret += 'qsize:'+str(q_accept_th.queue.qsize())
					except:
						q_accept_th.log_error("Failed to get Queue size during 'inf' command",2)
						ret += 'qsize:fail'
					
					#Get number of active threads
					try:
						q_accept_th.list_lock.acquire()
						ret += "|th_count:"+str(len(q_accept_th.tlist))
					finally:
						q_accept_th.list_lock.release()
					
					#Get stat counter values
					try:
						q_accept_th.stat_lock.acquire()
						
						ret += "|total_cmd:"+str(q_accept_th.total_count)
						ret += "|mkd_cmd:"+str(q_accept_th.mkd_count)
						ret += "|upl_cmd:"+str(q_accept_th.upl_count)
						ret += "|cpo_cmd:"+str(q_accept_th.cpo_count)
						ret += "|del_cmd:"+str(q_accept_th.del_count)
						ret += "|get_cmd:"+str(q_accept_th.get_count)
						ret += "|uptime:"+str(time.time() - q_accept_th.time_started)
						
					finally:
						q_accept_th.stat_lock.release()
					
					self.client.send(base64.b64encode(ret)+"\n")
					
				elif msg == '': #Client's done transmitting
					break
				else: #Nice try
					q_accept_th.log_error('Invalid instruction received: '+msg.strip().replace('\n',' '),1)
					self.client.send(base64.b64encode('invalid')+"\n")
			
		except:
			q_accept_th.log_error("Error while processing client instruction",3)
		finally: #When client confirms End Of Transmission, or just disconnects
			self.client.close()
		
		try:
			#Take ourselves off the thread list
			q_accept_th.list_lock.acquire()
			q_accept_th.tlist.remove(self)
			
			#Do we need to call for a new thread?
			if len(q_accept_th.tlist) == q_accept_th.maxthreads - 1:
				q_accept_th.th_event.set()
				q_accept_th.th_event.clear()
				
		except:
			q_accept_th.log_error("Error while finishing up thread execution (Perhaps problems with Event firing?)",3)
		finally:
			q_accept_th.list_lock.release()
	
	
	@staticmethod
	def newthread(client):
		q_accept_th.debug_check('Creating new thread')
		
		try:
			q_accept_th.list_lock.acquire()
			t = q_accept_th(client, len(q_accept_th.tlist))
			q_accept_th.tlist.append(t)
			q_accept_th.list_lock.release()
			t.start()
		except: q_accept_th.log_error("Error while creating new thread",3)
	
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
		
		q_accept_th.debug_log_lock.acquire()
		log = "["+time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime())+"] - "+type+": "+str(msg)+"\n"
		try:
			f = file(s3_config.log_folder+'/s3_queue_debug.log','a+')
			try:
				f.write(log)
			finally:
				f.close()
		except: pass
		q_accept_th.debug_log_lock.release()
	
	#Debug log aliases
	@staticmethod
	def debug_check(msg):
		q_accept_th.log_debug(msg,1)
	
	@staticmethod
	def debug_step(msg):
		q_accept_th.log_debug(msg,2)
	
	
	#Log generic event to log
	@staticmethod
	def log_event(msg):
		q_accept_th.event_log_lock.acquire()
		log = "["+time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime())+"]: "+str(msg)+"\n"
		try:
			f = file(s3_config.log_folder+'/s3_queue_event.log','a+')
			try:
				f.write(log)
			finally:
				f.close()
		except: pass
		q_accept_th.event_log_lock.release()

	#Log an error of a certain level (severity)
	# level 1: Notice, or unexpected behaviour (non-fatal, recoverable)
	# level 2: Warning. (non-fatal, recoverable)
	# level 3: Error. (Fatal, non-recovrable)
	@staticmethod
	def log_error(msg,level=1):
		q_accept_th.error_log_lock.acquire()
		
		if level == 1:
			type = 'NOTICE'
		elif level == 2:
			type = 'WARNING'
		else:
			type = 'ERROR'
		
		log = "["+time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime())+"] - "+type+": "+str(msg)+"\n"
		try:
			f = file(s3_config.log_folder+'/s3_queue_error.log','a+')
			try:
				f.write(log)
			finally:
				f.close()
		except: pass
		q_accept_th.error_log_lock.release()


	
#The daemon itself
class S3QueueDaemon(s3_daemon.Daemon):
	
	def run(self):
		#Set up socket for accepting connections
		for i in range(3):
			try:
				lstn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				lstn.bind((s3_config.queue_server_ip,s3_config.queue_server_port))
				lstn.listen(5)
				break
			except:
				if i == 2:
					q_accept_th.log_error('Queue server failed to initialize network socket connection. Shutting down...',3)
					return
				else:
					q_accept_th.log_error('Queue server failed to bind to IP '+str(s3_config.queue_server_ip)+' on Port '+str(s3_config.queue_server_port),2)
					time.sleep(30)
		
		q_accept_th.debug_check('Server up and listening')
		
		while True:
			try:
				#Accept new connection
				(client,ap) = lstn.accept()
				
				try:
					q_accept_th.list_lock.acquire()
					if len(q_accept_th.tlist) >= q_accept_th.maxthreads: #We're maxed out
						q_accept_th.log_error('Reached max threads, waiting for one to finish',2)
						q_accept_th.list_lock.release()
						q_accept_th.th_event.wait() #Wait for a thread to finish
					else:
						q_accept_th.list_lock.release()
					
				except:
					q_accept_th.log_error('Error while testing available threads',3)
					try:
						q_accept_th.list_lock.release()
					except: pass
					raise Exception("List lock error")
				
				q_accept_th.newthread(client)
				
			except: q_accept_th.log_error('Error while setting up new client connection',3)


#########################################################################
#########################################################################
# DAEMON STUFF

if __name__ == "__main__":
	daemon = S3QueueDaemon(s3_config.pid_path+'/s3_queue_daemon.pid')
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