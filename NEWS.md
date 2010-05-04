# S3 APU Updates and Changelog

## S3 APU 1.2.6.3 - 09/02/2010

### Major internal changes and New features

* **Worker** no longer uses **Multi cURL** internally. Instead it was replaced by
	sequential cURL calls, which solved the issue of high CPU usage while uploading
	to S3

* Added support for **COPY** operations within S3


********************************************************************************
### Minor features

* Added library extension and utility for retrieving META headers for
	an S3 object

* Added alternative listing utility, for lexical key-based object listing. This
	is essentially a standard listing utility that uses no separator

* PID files are now stored under _/var/run_ by default (configurable)

* Logging of unexpected errors during instruction handling in the **Worker**

* More solid regular-expression based instruction validation in Queue Daemon.
	This is for added security and stability in environments where external (not S3
	APU based) services are adding items to the Queue

* Standard list utility can now list from an alternative bucket


********************************************************************************
### Bug fixes

* Communication errors during the cURL perform phase are now caught

* Common prefixes are now included in the standard S3 listing output

* The Worker no longer skips a whole block of instructions if there's just a
	single missing file in there