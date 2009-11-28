#!/usr/bin/env python
#
# Get Status for running Q Daemon

import s3_config
import base64, socket

try:
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((s3_config.queue_server_ip, s3_config.queue_server_port))
	
	sock.send(base64.b64encode("inf")+"\n")
	ret = base64.b64decode(sock.recv(1024)).strip()

	print ret
	
except:
	print "Failed to get Q Status. Perhaps daemon not running?"