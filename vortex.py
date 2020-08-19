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
import statistics

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
## 8. 'executors': the number of executors to allocate for each dataset size batch-writing
##			Reconfigures the Spark 'spark.dynamicAllocation.maxExecutors' option. When 'executors'
##			is specified, the 'spark.dynamicAllocation.enabled' is set to True automatically.
## 9. 'executorMemory' = the memory allocated to each executor for a specific dataset size batch-write.
##			Reconfigures the Spark 'spark.executor.memory' option.
####
#### All 9 variables above can be found with the RECONFIGURABLE keyword (in a comment above / below).

#### 1. RECONFIGURABLE: datasets_bucket
datasets_bucket = ''
#### 2. RECONFIGURABLE: datasets_folder
datasets_folder = 'datasets'

datasets_location = "gs://"+datasets_bucket+"/"+datasets_folder+"/"

### 3. RECONFIGURABLE: dataset_sizes : choose from [3, 20, 100, 300, 1000]
dataset_sizes = [3, 20, 100, 300, 1000]

### 4. RECONFIGURABLE: Spark ds_format
ds_format = "com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2"

### 5. RECONFIGURABLE: num_tests_for_each
num_tests_for_each = 10

### 6. RECONFIGURABLE: repartition_mode (True / False)
repartition_mode = True

### 7. RECONFIGURABLE: max_stream_quota
max_stream_quota = 100

if datasets_bucket == '':
	raise Exception("String variable \"datasets_bucket\" cannot be empty.\nPlease create a Google Cloud Storage bucket, or retrieve the name of your current Google Cloud Storage bucket.\nThen, navigate to your python text editor, open file \"vortex.py\" and find variable \"datasets_bucket\" on line 53.\nSet this string variable it to your Google Cloud Storage bucket name.")

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
		    executors = '2'			#### 8 RECONFIGURABLE. Can add an 'executorMemory' (option 9) configuration as well
		elif size == 20:
		    executors = '4'			#### 8 RECONFIGURABLE. Can add an 'executorMemory' (option 9) configuration as well.
		elif size == 100:
		    executors = '40'		#### 8 RECONFIGURABLE. Can add an 'executorMemory' (option 9) configuration as well.
		elif size == 300:
		    executors = '120'		#### 8 RECONFIGURABLE. Can add an 'executorMemory' (option 9) configuration as well.
		elif size == 1000:
			executors = '250'		#### 8 RECONFIGURABLE
			executorMemory = '17g'	#### 9  RECONFIGURABLE
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


		temppath = temp_folder+str(size)+"GB"
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