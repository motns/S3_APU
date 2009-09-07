#!/usr/bin/env python
import s3_listing
import s3_upload

#s3_listing.list_objects("cdn-mdf",prefix="mdf_images/",marker="",maxkeys=10,delimiter="/")
#s3_listing.get_object("cdn-mdf","test.txt")
s3_upload.upload_object("cdn-mdf","/Users/adamb/Sites/s3_python_api/dummy.txt","dummy.txt","text/plain")
