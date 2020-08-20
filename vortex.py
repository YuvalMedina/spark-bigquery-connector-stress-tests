# Copyright 2020 Google Inc. All Rights Reserved.                              
#                                                                              
# Licensed under the Apache License, Version 2.0 (the "License");              
# you may not use this file except in compliance with the License.             
# You may obtain a copy of the License at                                      
#                                                                              
#       http://www.apache.org/licenses/LICENSE-2.0                             
#                                                                              
# Unless required by applicable law or agreed to in writing, software          
# distributed under the License is distributed on an "AS IS" BASIS,            
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.     
# See the License for the specific language governing permissions and          
# limitations under the License.                                               

import pyspark
from pyspark.sql import SparkSession
import time
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
import sys
import argparse

#### Notes:
## - because this test recreates the SparkSession for every different size data set,
##			the Spark UI on Dataproc will show the tests for each size in multiple separate 
## 			Spark applications.
## - this stress test is meant to be as reconfigurable as possible, as such there are some
##			variables that can be reconfigured below.

#### Instructions: This is a test to run 3, 20, 100, 300, and 1000GB batch writes on the
####			   Spark-BigQuery connector's writesupport integration.
#### Here are descriptions for the test variables to reconfigure:
## 1. Datasets GCS bucket location: 'datasets_bucket'
## 2. Datasets folder location within GCS bucket: 'datasets_folder'
## 3. Dataset sizes (also the name of each dataset [3GB, 20GB, etc.]): 'dataset_sizes'
## 4. Spark DataSource format (only change this if you know what you're doing or if the Spark 
##			DataSource name was updated): 'ds_format'
## 5. The number of tests to run for each size: num_tests_for_each
## 6. Repartition to the max stream quota, or not? :  repartition_mode
## 7. The quota for the maximum number of streams (if in repartition_mode): max_stream_quota
####
#### Here are the reconfigurable Spark options for each dataset size (set these up below in 
#### the writing loop, for each size. The default for each option is the empty string ''):
## 8. 'executors': the maximum number of executors to allocate for each dataset size batch-writing job.
##			Reconfigures the Spark 'spark.dynamicAllocation.maxExecutors' option. When 'executors'
##			is specified, the 'spark.dynamicAllocation.enabled' is set to True automatically.
## 9. 'executorMemory' = the memory allocated to each executor for a specific dataset size batch-write.
##			Reconfigures the Spark 'spark.executor.memory' option.
####
#### All 9 variables above can be found with the RECONFIGURABLE keyword (in a comment above / below).

parser = argparse.ArgumentParser(description='Process all optional parameters')
parser._action_groups.pop()
requiredBucket = parser.add_argument_group('required bucket argument.')
optional = parser.add_argument_group('all other optional arguments.')
requiredBucket.add_argument('datasets_bucket', help='your GCS storage bucket containing the datasets')
optional.add_argument('--datasets_folder', help='Your GCS storage folder containing the datasets within your bucket. The default is \'datasets\'')
optional.add_argument('-dataset_sizes','--dataset_sizes', nargs='+', type=int, help='The sizes you want to test on: please list as: \"--dataset_sizes 3 20 100 300 1000\". The default is all of these data sizes.')
optional.add_argument('--ds_format', help='The DataSource implementation format name to be used: only change if you know what you\'re doing.')
optional.add_argument('--num_tests_for_each', type=int, help='The number of tests to be run on each data size. The default is 10.')
optional.add_argument('--repartition_mode', type=bool, help='Whether datasets should be repartitioned or not. The default is True.')
optional.add_argument('--max_stream_quota', type=int, help='The current max stream quota in order to repartition for. The default is currently 100.')
optional.add_argument('--executors', nargs='+', help='The maximum number of executors to be run on each data-size. Please pass as \"--executors 1 null 3 4 null\". Use null to tell the program to use the default value for each data-size. If executors is specified, it must match the default (5) or specified length of dataset_sizes.')
optional.add_argument('--executorMemory', nargs='+', help='The memory allocated to each executor for each data-size. Please pass as \"--executorMemory 1m null 3g 4g null\". These are formatted as a number followed by a size unit suffix (\"k\", \"m\", \"g\" or \"t\"). Use null to tell the program to use the default value for each data-size. If executorMemory is specified, it must match the default (5) or specified length of dataset_sizes.')

args = parser.parse_args()

#### 1. RECONFIGURABLE: datasets_bucket
datasets_bucket = args.datasets_bucket
#### 2. RECONFIGURABLE: datasets_folder
datasets_folder = 'datasets' if args.datasets_folder is None else args.datasets_folder

datasets_location = "gs://"+datasets_bucket+"/"+datasets_folder+"/"

### 3. RECONFIGURABLE: dataset_sizes : choose from [3, 20, 100, 300, 1000]
dataset_sizes = [3, 20, 100, 300, 1000] if args.dataset_sizes is None else args.dataset_sizes
if args.executors is not None:
	assert len(args.executors) == len(dataset_sizes)
if args.executorMemory is not None:
	assert len(args.executorMemory) == len(dataset_sizes)

### 4. RECONFIGURABLE: Spark ds_format
ds_format = "com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2" if args.ds_format is None else args.ds_format

### 5. RECONFIGURABLE: num_tests_for_each
num_tests_for_each = 10 if args.num_tests_for_each is None else args.num_tests_for_each

### 6. RECONFIGURABLE: repartition_mode (True / False)
repartition_mode = True if args.repartition_mode is None else args.repartition_mode

### 7. RECONFIGURABLE: max_stream_quota
max_stream_quota = 100 if args.max_stream_quota is None else args.max_stream_quota

writesupport_type = "Vortex"

spark = SparkSession.builder.appName(writesupport_type).getOrCreate()
client = bigquery.Client()

project = client.project
bq_dataset = "spark_bigquery_"+writesupport_type+"_it_"+str(int(time.time()))

bq_datasetObj = bigquery.Dataset("{}.{}".format(project, bq_dataset))
client.create_dataset(bq_datasetObj)

storage_client = storage.Client()

# temp folder is a temporary place to write to gcs for caching purposes:
temp_folder = datasets_location+'temp/'

try:
	for size in dataset_sizes:
		executors = ''
		executorMemory = ''
		#### 8 / 9 RECONFIGURABLE 'executors' and 'executorMemory' for each datasize in this section below:
		if size == 3:
		    executors = '2' if args.executors is None or args.executors[0] == "null" else args.executors[0]
		    executorMemory = '' if args.executorMemory is None or args.executorMemory[0] == "null" else args.executorMemory[0]
		elif size == 20:
		    executors = '4' if args.executors is None or args.executors[1] == "null" else args.executors[1]
		    executorMemory = '' if args.executorMemory is None or args.executorMemory[1] == "null" else args.executorMemory[1]
		elif size == 100:
		    executors = '40' if args.executors is None or args.executors[2] == "null" else args.executors[2]
		    executorMemory = '' if args.executorMemory is None or args.executorMemory[2] == "null" else args.executorMemory[2]
		elif size == 300:
		    executors = '120' if args.executors is None or args.executors[3] == "null" else args.executors[3]
		    executorMemory = '' if args.executorMemory is None or args.executorMemory[3] == "null" else args.executorMemory[3]
		elif size == 1000:
			executors = '250' if args.executors is None or args.executors[4] == "null" else args.executors[4]
			executorMemory = '17g' if args.executorMemory is None or args.executorMemory[4] == "null" else args.executorMemory[4]
		#### 8 / 9 RECONFIGURABLE options above

		configOptions = []
		if executors != '':
			configOptions.append(('spark.dynamicAllocation.enabled', 'true'))
			configOptions.append(('spark.dynamicAllocation.maxExecutors', executors))
		if executorMemory != '':
			configOptions.append(('spark.executor.memory', executorMemory))
		spark.sparkContext.stop()
		conf = pyspark.SparkConf().setAll(configOptions)
		spark = SparkSession.builder.config(conf=conf).appName(writesupport_type).getOrCreate()

		readpath = datasets_location+str(size)+"GB"

		if repartition_mode:
			# if size is 3GB dataframe doesn't need any repartitioning either way.
			if size == 3:
				DF = spark.read.parquet(readpath).cache()
			else:
				DF = spark.read.parquet(readpath).repartition(max_stream_quota).cache()
		else:
			DF = spark.read.parquet(readpath).cache()


		temppath = temp_folder+str(size)+"GB"+str(int(time.time()))
		DF.write.parquet(temppath);

		partitions = DF.rdd.getNumPartitions()

		timestamps = []

		for i in range(0, num_tests_for_each):
			tick = datetime.now()

			writeTo = str(int(time.time()))
			DF.write.format(ds_format).option('table', writeTo).option('dataset', bq_dataset).save();

			tock = datetime.now()

			diff = tock - tick

			timestamps.append(diff.total_seconds())

		for timestamp in timestamps:
			print('{}, {}, {}, {}'.format(writesupport_type, size, timestamp, partitions))
except Exception as e:
	print(e)


client.delete_dataset(
	bq_datasetObj, delete_contents=True, not_found_ok=False
)

gcs_bucket = storage_client.get_bucket(datasets_bucket)
# list all objects in the temporary folder in GCS
blobs = gcs_bucket.list_blobs(prefix=temp_folder)
# delete them all.
for blob in blobs:
	blob.delete()