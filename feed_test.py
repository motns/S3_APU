import s3_feeder

feeder = s3_feeder.Feeder("/Users/adamb/s3_test","s3_test",2,1)
feeder.run()