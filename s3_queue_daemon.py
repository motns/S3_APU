#!/usr/bin/env python

import s3_daemon
import s3_config
import socket, sys, threading, Queue, base64, time


#########################################################################
#########################################################################
# SERVER STUFF

#Thread for accepting instructions
class q_accept_th(threading.Thread):
	tlist = [] # list of all current accept threads
	maxthreads = 100 # max number of threads we're allowing
	
	th_event = threading.Event() # event to signal OK to create more threads
	list_lock = threading.Lock() # lock to guard tlist
	
	#Locks for shared logging
	event_log_lock = threading.Lock()
	error_log_lock = threading.Lock()
	debug_log_lock = threading.Lock()
	
	#The item list
	queue = Queue.Queue()
	
	def __init__(self,client,id):
		threading.Thread.__init__(self)
		self.threadnum = id # thread ID
		self.client = client
		
	def run(self):
		q_accept_th.debug_step('Thread ID '+str(self.threadnum)+" running")
		
		try:
			
			msg = base64.b64decode(self.client.recv(1024))
			q_accept_th.debug_step('Received message: '+msg)
			
			#Validate instruction received
			#
			# mkdir - create folder in S3 (mkdir|path/to/dir)
			# upload - upload file into S3 (upload|/path/to/file|/path/in/s3) 
			# getone - pop out the last item in the queue
			
			cmd = msg[0:3]
			q_accept_th.debug_step('Extracted command: '+cmd)
			
			if cmd == 'mkd':
				try:
					q_accept_th.queue.put_nowait(msg.split("|"))
					
					#Confirm
					qs = q_accept_th.queue.qsize()
					self.client.send(base64.b64encode('ok - '+str(qs)))
				except:
					self.client.send(base64.b64encode('fail'))
				
			elif cmd == 'upl':
				try:
					q_accept_th.queue.put_nowait(msg.split("|"))
					
					#Confirm
					qs = q_accept_th.queue.qsize()
					self.client.send(base64.b64encode('ok - '+str(qs)))
				except:
					self.client.send(base64.b64encode('fail'))
				
			elif cmd == 'get':
				try:
					item = q_accept_th.queue.get_nowait()
					ret = '|'.join(item)
				except:
					ret = ''
					
				self.client.send(base64.b64encode(ret))
				
			else:
				self.client.send(base64.b64encode('invalid'))
			
		finally:
			self.client.close() #Clean up after ourselves
		
		#Take ourselves off the list	
		q_accept_th.list_lock.acquire()
		q_accept_th.tlist.remove(self)
		
		#Do we need to call for a new thread?
		if len(q_accept_th.tlist) == q_accept_th.maxthreads - 1:
			q_accept_th.th_event.set()
			q_accept_th.th_event.clear()
			
		q_accept_th.list_lock.release()
	
	
	def newthread(client):
		q_accept_th.debug_check('Creating new thread')
		
		q_accept_th.list_lock.acquire()
		t = q_accept_th(client, len(q_accept_th.tlist))
		q_accept_th.tlist.append(t)
		q_accept_th.list_lock.release()
		t.start()
	newthread = staticmethod(newthread)
	
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
		log = "["+time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime())+"] - "+type+": "+msg+"\n"
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
		log = "["+time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime())+"]: "+msg+"\n"
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
		
		log = "["+time.strftime("%a, %d %b %Y %H:%M:%S %Z",time.gmtime())+"] - "+type+": "+msg+"\n"
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
		lstn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		lstn.bind(('127.0.0.1',19998))
		lstn.listen(5)
		
		q_accept_th.debug_check('Server up and listening')
		
		while True:
			q_accept_th.debug_step('Waiting for client')
			
			#Accept new connection
			(client,ap) = lstn.accept()
			q_accept_th.debug_step('Client connected')
			
			q_accept_th.list_lock.acquire()
			if len(q_accept_th.tlist) >= q_accept_th.maxthreads: #We're maxed out
				q_accept_th.debug_check('Reached max threads, waiting for one to free up')
				q_accept_th.list_lock.release()
				q_accept_th.th_event.wait() #Wait for a thread to finish
			else:
				q_accept_th.list_lock.release()
			
			q_accept_th.newthread(client)


#########################################################################
#########################################################################
# DAEMON STUFF


#Check run command
if __name__ == "__main__":
	daemon = S3QueueDaemon('/tmp/s3_queue_daemon.pid')
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