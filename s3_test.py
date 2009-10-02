#!/usr/bin/env python
import s3_listing
import s3_upload

#s3_listing.list_objects("cdn-mdf",prefix="mdf_images/",marker="",maxkeys=10,delimiter="/")
#s3_listing.get_object("cdn-mdf","mdf_images/0/100/10020/10020_new.jpg")
#s3_listing.get_object("cdn-mdf","mdf_images")
s3_upload.upload_object("cdn-mdf","upl","test_dir/dummy.txt","/Users/adamb/Sites/dummy.txt")
#s3_upload.upload_object("cdn-mdf","mkd","test_dir")