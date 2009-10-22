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

#cmd = 'get|3'
#cmd = 'inf'
#cmd = 'mkd|/tst/dir'
#cmd = 'upl|/tst/dir/file.jpg'

work_list = [
	"mkd|test_images2",
	"mkd|test_images2/images1",
	"mkd|test_images2/images2",
	"upl|/Users/adamb/Documents/guitar_wallpaper/08-Prestige_L.jpg|test_images2/images1/P1030596.JPG",
	"upl|/Users/adamb/Documents/guitar_wallpaper/RG-Creations_L.jpg|test_images2/images1/P1040818.JPG",
	"upl|/Users/adamb/Documents/guitar_wallpaper/RGR320EX_L.jpg|test_images2/images2/ovi_csoport.jpg",
	"upl|/Users/adamb/Documents/guitar_wallpaper/RGT320F_L.jpg|test_images2/images2/profile_cool.jpg",
	"upl|/Users/adamb/Documents/guitar_wallpaper/RG_Prestige_L.jpg|test_images2/images2/profile_prof.jpg"
]

for work in work_list:
	
	#cmd = ''
	#cmd = 'mkd|/tst/dir'
	#cmd = 'upl|/tst/dir/file.jpg|dir/file.jpg'
	#cmd = 'del|dir/file.jpg'
	#cmd = 'inf'
	#cmd = 'invalid'
	#cmd = 'get|3'
	
	s.send(base64.b64encode(work)+"\n")
	v = base64.b64decode(s.recv(1024))
	print v

s.close() # close socket