#!/ usr/bin/env python
#simple illustration of thread module 
# multiple clients connect to server; each client repeatedly sends a

# letter k, which the server adds to a global string v and echos back 
# to the client; k = '' means the client is dropping out; when all 
# clients are gone, server prints final value of v 
 
# this is the server 
 
import socket # networking module 
import sys 
 
import thread 
 
# note the globals v and nclnt, and their supporting locks, which are 
# also global; the standard method of communication between threads is 
# via globals 
 
# function for thread to serve a particular client, c 
def serveclient(c): 
	global v,nclnt,vlock,nclntlock 

	while 1: 
		# receive letter from c, if it is still connected 
		k = c.recv(1) 
		if k == '': break 
		
		# concatenate v with k in an atomic manner, i.e. with protection 
		# by locks 
		vlock.acquire() 
		v += k 
		vlock.release() 

		# send new v back to client 
		c.send(v)
	c.close() 
	nclntlock.acquire() 
	nclnt -= 1 
	nclntlock.release() 

# set up Internet TCP socket 
lstn = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
 
port = 19998 # server port number 
# bind lstn socket to this port 
lstn.bind(('127.0.0.1', port)) 

# start listening for contacts from clients (at most 2 at a time) 
lstn.listen(5) 

# initialize concatenated string, v 
v = ''
# set up a lock to guard v 
vlock = thread.allocate_lock() 

# nclnt will be the number of clients still connected 
nclnt = 2 
# set up a lock to guard nclnt 
nclntlock = thread.allocate_lock() 
 
# accept calls from the clients 
for i in range(nclnt): 

	# wait for call, then get a new socket to use for this client, 
	# and get the client's address/port tuple (though not used) 
	clnt, ap = lstn.accept() 
	# start thread for this client, with serveclient() as the thread's 
	# function, with parameter clnt; note that parameter set must be 
	# a tuple; in this case, the tuple is of length 1, so a comma is 
	# needed 
	thread.start_new_thread(serveclient,(clnt,)) 

# shut down the server socket, since it's not needed anymore 
lstn.close() 

# wait for both threads to finish 

while nclnt > 0: pass  
print 'the final value of v is', v