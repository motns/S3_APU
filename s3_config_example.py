#!/usr/bin/env python
#You have to create a config file: s3_config.py

#Server TCP settings
queue_server_ip = "127.0.0.1"
queue_server_port = 19998

#S3 Access details
aws_access_key_id = ""
aws_secret_key = ""
s3_bucket = ""

#Folder to store all log files
log_folder = '/path/to/folder'

#Debug Level
# 0: None
# 1: Checkpoints
# 2: Checkpoints + Steps
debug_level = 0

#Maximum worker threads
max_workers = 1

#Maximum number of files to attempt to upload in one go
max_files = 5

#Maximum number of retries during transactions
max_retries = 3

#How long we should apply the SlowDown lock for (in seconds)
slowdown_time = 15.0

#Group and User IDs for file uploads
upload_uid = 1
upload_gid = 1

#The location for storing the Process ID file
# "/var/run" should work on most systems, but this might have to be tweaked
pid_path = "/var/run"