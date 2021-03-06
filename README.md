# S3_APU

Asynchronous Upload tool for Amazon's Simple Storage Service (S3), written in Python

* **Author**: Adam Borocz (http://github.com/motns)
* **Version**: 1.2.6.3
* **License**: GPL version 3

## Table of Contents

1. **Description**

2. **Components**

	2.1 S3 Queue Daemon (s3_queue_daemon)
	2.2 S3 Worker Daemon (s3_worker_daemon)
	2.3 S3 Feeder

3. **Installation**

	3.1 Requirements
	3.2 Initial Configuration
	3.3 Starting Services

4. **Usage**

	4.1 The feeder utility (util_feed)
	4.2 The list utility (util_list)
	4.3 The alternative list utility (util_list_noseparator)
	4.4 The status utility (util_q_status)

5. **Internal "instruction" API**

	5.1 mkd
	5.2 upl
	5.3 cpo
	5.4 del
	5.5 get
	5.6 inf


## 1\. Description

S3 APU is a Python-based system for uploading files and folders into Amazon S3
in an asynchronous fashion. It's designed to transfer a large number of objects
at a very high rate, while also being fault tolerant. It also uses a compatible
layout in S3, so that popular implementations (like the ones in CyberDuck or s3fs)
can view it properly. The system is essentially based on the interaction of
3 components: **Queue**, **Worker** and **Feed** (see section 3 for details).


## 2\. Components

### 2\.1 S3 Queue Daemon (s3_queue_daemon)

A socket-based, multi-threaded, asynchronous queue service. It binds to localhost
on a specified port, and accepts "instructions" from producers. The service
accepts only a specific set of instructions (detailed later), which are validated
and then entered into the queue (based on the **Queue** Python module).

When a consumer (Worker Daemon) sends a request for work, the Queue issues a
blocking request for fetching items from the list. This way if there's no work to
be done at that precise moment, the worker will stay connected in a hanging
connection (for 10s), waiting for an instruction to appear on the list. This is
to improve response time, since this is an asynchronous service afterall.

The worker may request multiple items from the queue (up to 30 at the moment),
which will be returned as a single response, separated by a ';'. When the worker
requests more items than what's currently available, the queue will only make it
wait for the first one. This way the worker won't sit around wasting time, when
it's got better things to do. ;)

The queue daemon also provides information about it's internal status, which can
be accessed using the **Queue Status** utility (see section 4.4)


********************************************************************************
### 2\.2 S3 Worker Daemon

A multi-threaded upload service based on cURL (PyCurl). It connects to the
S3 Queue service, and retrieves a list of instructions to execute against S3.
These will tell the worker to either:
* Create a new folder
* Upload a file
* Delete an object

The number of instructions it retrieves in one go, and the number of concurrent
worker threads can be adjusted through the **max_files** and **max_workers** variables
in the configuration file.

It pipes requests in a single connection, so when there's a large number of files
going up in one go, it could help to have the **max_workers** low (1 or 2), and the
**max_files** high (like 20). This will maximize throughput, by reducing the overhead
of establishing a connection each time.
However, this could reduce the responsiveness of the uploader, since it won't
grab new instructions until it's done with the current ones. To make it faster
to respond (but slower to upload), just increase the **max_workers** (to 5 or 6),
and reduce the 'max_files' (to 5 or so).

The Worker daemon will handle any errors coming from S3, and will log them in the
_s3_worker_error_ log file. If the error is recoverable (e.g. a network timeout),
**S3 APU** will re-attempt the transaction. The number of retries can be configured
through the **max_retries** variable.
Also, if the worker hits a 503 error (Amazon asking it to slow down), it will go
into _backoff mode_ (cut back the request rate) for a specific amount of time,
controlled by the **slowdown_time** variable.

Successful transactions will be logged in the _s3_worker_event_ log. The worker also
outputs and updates a file with it's internal state (**s3_worker_performance**),
located in the S3 APU log folder.

> **PLEASE NOTE**: Currently all objects are uploaded as publicly accessible!


********************************************************************************
### 2\.3 S3 Feeder

The S3 Feeder is a class, used to work sort of like the rsync utility in Linux.
It can be initiated through the **util_feed** utility, and its purpose is to take
a folder, and feed its contents into the S3 Queue.
It has facilities for checking whether an object is already uploaded, so it won't
be wasting bandwidth. It also has a setting for recursion depth, so it can do just
the current folder, or traverse through the entire tree.


## 3\. Installation

### 3\.1 Requirements

If you don't have an **Amazon S3** account (or have no idea what it is), perhaps you
should first have a look around on this site: http://aws.amazon.com/s3

S3 APU requires in order to run:

* Python 2.5.2 or higher
* PyCurl 7.18.2 or higher (available from: http://pycurl.sourceforge.net/)

> **PLEASE NOTE**: It may actually work on earlier versions of PyCurl, but 7.18.2 is the
> oldest one that S3 APU was tested with.


********************************************************************************
### 3\.2 Initial Configuration

To run **S3 APU**, a configuration file needs to be created first. There's an
example located in the main folder (as _s3_config_example_) which can be used. Just
copy its contents into a file called _s3_config.py_ and go from there.

The **Amazon S3** access keys, and the default bucket name need to be entered in the
configuration file first for the uploader to work.
**S3 APU** only supports one destination bucket for uploading at the moment.

> **PLEASE NOTE**: If changes are made to the configuration file, they won't
> take effect until the services are restarted.


********************************************************************************
### 3\.3 Starting Services

When ready, just fire up the **Queue** and the **Worker** (from the **S3_APU** root folder):

	./s3_queue_daemon start
	./s3_worker_daemon start

Optionally (on Red Hat, Fedora and Cent OS), these two files can be symlinked into 
the _/etc/init.d_ folder, so the **chckconfig** and **service** commands can be used
to manage them.



## 4\. Usage

### 4\.1 The feeder utility (util_feed)

The feeder utility is a CLI front-end to the **S3 Feeder** class. It takes its arguments
in the following format:

	./util_feed FEED_ROOT [KEY_ROOT] [RECURSION] [CHECK_MISSING]

* **FEED_ROOT**
	The folder to start feeding from. This will take the contents of the specified folder,
	and traverse down from there to a depth defined by RECURSION.

* **KEY_ROOT (Defaults to empty string)**
	Allows the user to define a 'folder' prefix (without a trailing '/'), under which
	the uploaded objects will go. So for key root 'testing', the 'file' objects will
	have this key: 'testing/file'.

* **RECURSION (Defaults to 1)**
	How far down should the feeder go:
		_0_ : Go down all the way
		_1_ : Only do the current folder (**FEED_ROOT**)
		_> 1_ : Go down to specified depth

* **CHECK_MISSING (Defaults to 0)**
Whether to upload only missing objects into S3. Not checking will make the feeding
much faster, but might waste bandwidth by uploading existing objects again.


********************************************************************************
### 4\.2 The standard list utility (util_list)

Very simple CLI front-end to the internal S3 listing function. It will list all
the keys with a certain prefix, using "/" as a separator:

	./util_list PREFIX [LIMIT] [BUCKET]

* **PREFIX**
	The Key prefix to use, without a trailing '/'. So to list the contents of the
	folder 'test' in s3:

		./util_list test

*  **LIMIT** _(defaults to 0)_
	Limits the number of keys returned:
		_0_ : Get all the keys (Could take a while for large folders!)
		_> 1_ : Get specified amount

* **BUCKET**
	An alternative bucket to list objects from. Please note however, that this will
	only work if the S3 credentials being used have permissions to access that bucket.

> **PLEASE NOTE**: This function will also compensate for the problem of S3 returning less than the
> requested number of keys, even through there's more available.


********************************************************************************
### 4\.3 The alternative list utility (util_list_noseparator)

Identical in functionality to the standard listing utility, except that it uses
no separator. This will essentially provide an unrestricted lexical listing of
object keys in the bucket.


********************************************************************************
### 4\.4 The status utility (util_q_status)

A simple utility, used to query the S3 Queue for it's current status. Just run
without arguments:

	./util_q_status

It returns data in the following format:

	qsize:0|th_count:1|total_cmd:0|mkd_cmd:0|upl_cmd:0|del_cmd:0|get_cmd:0|uptime:0.0

* _qsize_: The appximate number of items in the queue right now
* _th_count_: The number of queue threads sending/receiving instructions at the moment
* _total_cmd_: The total number of instructions received
* _mkd_cmd_: The total number of 'make directory' instructions
* _upl_cmd_: The total number of 'upload file' instructions
* _cpo_cmd_: The total number of 'copy object' instructions
* _get_cmd_: The total number of 'get work' instructions
* _uptime_: The number of seconds passed since the service was started



## 5\. Internal "instruction" API

The Queue and Worker services use a pre-defined set of 'instructions' to communicate.
Instructions begin with a 3 character command, and then contain a list of
parameters in a '|' separated format. The Queue receives and sends data in
base64 encoded format. After it receives a valid instruction, the queue will
respond with an 'ok' to confirm that it was successfully stored. Upon
failure the queue responds with 'fail', while validation errors will return 'invalid'.
Both errors and validation failures are also logged in the s3_queue_error log.

********************************************************************************
### 5\.1 mkd

Instructs the worker to create a new folder in S3. Since Amazon S3 has no concept
of folders as such, this command will create a 0-length object in S3, to mark the
location of a 'virtual' folder. It also appends a set of meta headers for
compatibility with current S3 access implementations (like s3fs).


#### Headers

	Content-Type: application/x-directory
	x-amz-acl: public-read
	x-amz-meta-uid: [As defined in s3_config]
	x-amz-meta-gid: [As defined in s3_config]
	x-amz-meta-mode: 040755 (Mode for folder node at 775 access permissions in Linux)


#### Usage

	mkd|path/for/newfolder

Will create object: _http://yourbucket.s3.amazonaws.com/path/to/newfolder_

> **PLEASE NOTE:** There's no '/' at the beginning! Otherwise you could end up with two
> slashes in your URL.


********************************************************************************
### 5\.2 upl

Instructs the worker to upload a file into S3. Takes the source path and
destination key as its two arguments. For performance reasons, the Queue service
will not check whether the source path exists. All the checking happens in the
Worker. The Worker will also try to determine the MIME type based on the extension,
and falls back to "text/plain" when it's unable to.

#### Supported types

* **jpeg, jpg, jpe**: _image/jpeg_
* **gif**: _image/gif_
* **png**_image/x-png_
* **tiff, tif**: _image/tiff_
* **html, htm**: _text/html_
* **css**: _text/css_
* **js**: _text/javascript_
* **pdf**: _application/pdf_
* **rtf**: _application/rtf_

The headers are almost the same as for a folder, except that the mode is adjusted.

#### Headers

	Content-Type: [As determined above]
	x-amz-acl: public-read
	x-amz-meta-uid: [As defined in s3_config]
	x-amz-meta-gid: [As defined in s3_config]
	x-amz-meta-mode: 0100755 (Mode for file node at 775 access permissions in Linux)

The Worker will also calculate the MD5 checksum for the source file and adds it
as a header. Amazon S3 uses this to check whether the file got corrupted during
transmission. It's also used to check if the file contents were changed when an
object is re-uploaded.


#### Usage

	upl|/path/to/source|key/for/destination

Will create object: _http://yourbucket.s3.amazonaws.com/key/for/destination_

> **PLEASE NOTE:** Once again, there's no '/' at the beginning.


********************************************************************************
### 5\.3 cpo

Instructs the worker to run a COPY operation on Amazon S3. It takes the source
bucket and key as its first argument, and the destination key as the second one.
It can copy from any bucket the configured user has access to, and will create
the resulting object in the bucket previously selected in the configuration file.

This will essentially run a _PUT_ operation without posting anything.


#### Headers

	x-amz-acl: public-read,
	x-amz-copy-source: [source in '/bucket/object' format]


#### Usage

	cpo|/bucket/object/path|new/object/path

Will create object: _http://yourbucket.s3.amazonaws.com/new/object/path_

> **PLEASE NOTE:** There IS a leading '/' in the source path (preceding the bucket name),
> but there ISN'T one in the destination object path.


********************************************************************************
### 5\.4 del

Used to delete an object from Amazon S3. Takes the destination key as it's
argument. Amazon responds the same way regardless of the destination key being
in existence. Therefore there's no need to check first for a key you want delete.

#### Headers
_None_


#### Usage

	del|key/to/delete

Will remove object: _http://yourbucket.s3.amazonaws.com/key/to/delete_

> **PLEASE NOTE:** Once again, there is no '/' at the beginning.


********************************************************************************
### 5\.5 get

Used to retrieve one or more items from the Queue. Takes the number of items as
it's only argument. The current maximum is 30 items, so passing in a bigger number
falls back to this.
When trying to get an item from an empty list, the Queue will make the conumer
wait for 10s before returning an empty result set.
When getting more than one key, they'll be returned as a ';'-separated string.

####Usage

	get|2

May return something like this:

	mkd|newfolder;upl|/source|destination


********************************************************************************
### 5\.6 inf

Used to get information about the internal state of the Queue. Takes no arguments.
For output, please refer to the util_q_status (4.3) documentation above.

#### Usage

	inf