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
	"mkd|test_images",
	"mkd|test_images2",
	"mkd|test_images3",
	"mkd|test_images/images1",
	"mkd|test_images2/images1",
	"mkd|test_images3/images1",
	"mkd|test_images/images2",
	"mkd|test_images2/images2",
	"mkd|test_images3/images2",
	"upl|/Users/adamb/Pictures/P1030596.JPG|test_images/images1/P1030596.JPG",
	"upl|/Users/adamb/Pictures/P1030596.JPG|test_images2/images1/P1030596.JPG",
	"upl|/Users/adamb/Pictures/P1030596.JPG|test_images3/images1/P1030596.JPG",
	"upl|/Users/adamb/Pictures/P1040818.JPG|test_images/images1/P1040818.JPG",
	"upl|/Users/adamb/Pictures/P1040818.JPG|test_images2/images1/P1040818.JPG",
	"upl|/Users/adamb/Pictures/P1040818.JPG|test_images3/images1/P1040818.JPG",
	"upl|/Users/adamb/Pictures/ovi_csoport.jpg|test_images/images2/ovi_csoport.jpg",
	"upl|/Users/adamb/Pictures/ovi_csoport.jpg|test_images2/images2/ovi_csoport.jpg",
	"upl|/Users/adamb/Pictures/ovi_csoport.jpg|test_images3/images2/ovi_csoport.jpg",
	"upl|/Users/adamb/Pictures/profile_cool.jpg|test_images/images2/profile_cool.jpg",
	"upl|/Users/adamb/Pictures/profile_cool.jpg|test_images2/images2/profile_cool.jpg",
	"upl|/Users/adamb/Pictures/profile_cool.jpg|test_images3/images2/profile_cool.jpg",
	"upl|/Users/adamb/Pictures/profile_prof.jpg|test_images/images2/profile_prof.jpg",
	"upl|/Users/adamb/Pictures/profile_prof.jpg|test_images2/images2/profile_prof.jpg",
	"upl|/Users/adamb/Pictures/profile_prof.jpg|test_images3/images2/profile_prof.jpg"
]

#work_list = [
#	"mkd|perf_test",
#	"mkd|perf_test/test_images1",
#	"mkd|perf_test/test_images1/folder1",
#	"mkd|perf_test/test_images1/folder2",
#	"mkd|perf_test/test_images2",
#	"mkd|perf_test/test_images2/folder1",
#	"mkd|perf_test/test_images2/folder2",
#	"upl|/newdrive/galleries/mdf_images/0/100/10020/10020.jpg|perf_test/test_images1/folder1/10020.jpg",
#	"upl|/newdrive/galleries/mdf_images/0/100/10020/10020_new.jpg|perf_test/test_images1/folder1/10020_new.jpg",
#	"upl|/newdrive/galleries/mdf_images/0/100/10020/10020_2009_156.jpg|perf_test/test_images1/folder2/10020_2009_156.jpg",
#	"upl|/newdrive/galleries/mdf_images/0/100/10022/10022_2008_222.jpg|perf_test/test_images2/folder1/10022_2008_222.jpg",
#	"upl|/newdrive/galleries/mdf_images/0/100/10022/10022_2008_231_2.jpg|perf_test/test_images2/folder1/10022_2008_231_2.jpg",
#	"upl|/newdrive/galleries/mdf_images/0/100/10022/10022_2008_239_2.jpg|perf_test/test_images2/folder1/10022_2008_239_2.jpg",
#	"upl|/newdrive/galleries/mdf_images/0/100/10022/10022_2008_240_2.jpg|perf_test/test_images2/folder1/10022_2008_240_2.jpg",
#	"upl|/newdrive/galleries/mdf_images/0/100/10022/10022_2008_244.jpg|perf_test/test_images2/folder2/10022_2008_244.jpg",
#	"upl|/newdrive/galleries/mdf_images/0/100/10022/10022_2008_242.jpg|perf_test/test_images2/folder2/10022_2008_242.jpg",
#	"upl|/newdrive/galleries/mdf_images/0/100/10022/10022_2008_231.jpg|perf_test/test_images2/folder2/10022_2008_231.jpg",
#	"upl|/newdrive/galleries/mdf_images/0/100/10022/10022_2008_239.jpg|perf_test/test_images2/folder2/10022_2008_239.jpg"
#]

#work_list = [
#	"mkd|test_images2",
#	"mkd|test_images2/images1",
#	"mkd|test_images2/images2",
#	"upl|/Users/adamb/Documents/guitar_wallpaper/08-Prestige_L.jpg|test_images2/images1/P1030596.JPG",
#	"upl|/Users/adamb/Documents/guitar_wallpaper/RG-Creations_L.jpg|test_images2/images1/P1040818.JPG",
#	"upl|/Users/adamb/Documents/guitar_wallpaper/RGR320EX_L.jpg|test_images2/images2/ovi_csoport.jpg",
#	"upl|/Users/adamb/Documents/guitar_wallpaper/RGT320F_L.jpg|test_images2/images2/profile_cool.jpg",
#	"upl|/Users/adamb/Documents/guitar_wallpaper/RG_Prestige_L.jpg|test_images2/images2/profile_prof.jpg"
#]

for work in work_list:
	
	s.send(base64.b64encode(work)+"\n")
	v = base64.b64decode(s.recv(1024))
	print v

s.close() # close socket
