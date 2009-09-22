#Simple client script, to test the Queue server

import socket # networking module 
import sys 
import base64

# create Internet TCP socket 
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 

host = '127.0.0.1' # server address 
port = 19998 # server port 

# connect to server 
s.connect((host, port)) 

print 'Connected...'
print 'Sending instruction'

cmd = 'get|3'
#cmd = 'inf'
#cmd = 'mkd|/tst/dir'
#cmd = 'upl|/tst/dir/file.jpg'

s.send(base64.b64encode(cmd))
v = base64.b64decode(s.recv(1024))
print v

s.close() # close socket