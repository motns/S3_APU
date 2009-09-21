#!/usr/bin/env python
#You have to create a config file: s3_config.py

#Server TCP settings
queue_server_ip = "127.0.0.1"
queue_server_port = 19998

#S3 Access details
aws_access_key_id = ""
aws_secret_key = ""

#Folder to store all log files
log_folder = '/path/to/folder'

#Debug Level
# 0: None
# 1: Checkpoints
# 2: Checkpoints + Steps
debug_level = 0