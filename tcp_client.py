# simple illustration of thread module 

# two clients connect to server; each client repeatedly sends a letter, 
# stored in the variable k, which the server appends to a global string 
# v, and reports v to the client; k = '' means the client is dropping 
# out; when all clients are gone, server prints the final string v 

# this is the client; usage is 

# python clnt.py server_address port_number 

import socket # networking module 
import sys 

# create Internet TCP socket 
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

host = '127.0.0.1' # server address 
port = 19998 # server port 

# connect to server 
s.connect((host, port)) 

while(1): 
	# get letter 
	k = raw_input('enter a letter:')
	s.send(k) # send k to server 
	# if stop signal, then leave loop 
	if k == '': break 
	v = s.recv(1024) # receive v from server (up to 1024 bytes) 
	print v 

s.close() # close socket
