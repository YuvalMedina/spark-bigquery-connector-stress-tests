import pyspark
from pyspark.sql import SparkSession
import time
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
import sys
import argparse

parser = argparse.ArgumentParser(description='Process all optional parameters')
parser._action_groups.pop()
requiredBucket = parser.add_argument_group('required bucket argument.')
optional = parser.add_argument_group('all other optional arguments.')
requiredBucket.add_argument('user_bucket', help='your GCS storage bucket containing the datasets')
optional.add_argument('--user_folder', help='Your GCS storage folder containing the datasets within your bucket. The default is \'datasets\'. If you specify this value, the folder \'datasets\' will be contained within the directory you specified.')

args = parser.parse_args()

#### 1. RECONFIGURABLE: datasets_bucket
write_bucket = args.user_bucket

#### 2. RECONFIGURABLE: datasets_folder
if args.user_folder is None:
	write_folder = 'datasets'
else:
	write_folder = args.user_folder+'/'+'datasets'

write_location = "gs://"+write_bucket+"/"+write_folder+"/"

dataset_sizes = [3, 20, 100, 300, 1000]

read_location = "gs://ymed-titanic-data/stress-test-datasets/datasets/"

spark = SparkSession.builder.getOrCreate()

for size in dataset_sizes:
	readpath = read_location+str(size)+"GB"
	writepath = write_location+str(size)+"GB"

	df = spark.read.parquet(readpath)
	df.write.parquet(writepath)