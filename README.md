# spark-bigquery-connector-stress-tests
Stress tests for the spark-bigquery-connector

**Instructions:**
			This is a test to run 3, 20, 100, 300, and 1000GB batch writes on the Spark-BigQuery connector's writesupport integration.
	This test is to be run with the following command:
		`gcloud dataproc jobs submit pyspark --cluster=<YOUR-CLUSTER> <LOCATION-OF-vortex.py> --jars=${CONNECTOR}`
	The location of the jar file above will change when the Spark <-> BigQuery WriteSupport integration is merged into the Spark Connector repository on GitHub, and is released.

**Before running this test:**
1. Please create / retrieve your Google Cloud Storage bucket name.
2. Please replace variable `datasets_bucket` on line 53 of the file `vortex.py` with your bucket's name.
3. Then please copy the datasets for the stress test into your bucket using the command `gsutil cp -r gs://ymed-titanic-data/stress-test-datasets <YOUR-BUCKET-NAME>` The data should show up in folder "datasets" in your Google Storage Bucket. Thus the default name for this folder should be "datasets".
		* However, if you specified an inner directory in <YOUR-BUCKET-NAME>, the folder "datasets" will be nested inside the directory. In this case, simply copy the folder location inside your bucket (such as 'folder1/folder2/.../datasets') into variable `datasets_folder` on line 55 in file `vortex.py`
4. Please create an appropriate cluster for testing. Your cluster must have pip packages `google-cloud-bigquery` and `google-cloud-storage` installed. A sample command to do this: 
	```
	gcloud dataproc clusters create <YOUR-CLUSTER-NAME> \
	  --region=$REGION \
	  --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh \
	  --metadata='PIP_PACKAGES=google-cloud-storage google-cloud-bigquery' \
	  --num-workers=<NUMBER-OF-WORKERS> \
	  --worker-machine-type=<WORKER-MACHINE-TYPE>
	```
	* Please set your current Dataproc region using `export REGION=<DATAPROC_REGION>` before running this command.
	* If you wish to run stress tests for up to 1000GB, the recommended number of workers is 250, and the recommended machine type is "n2-standard-8". If your project doesn't have enough allowance for this, you may want to create your cluster with 120 workers, and use the standard machine type of "n1-standard-4": with this, you may run write jobs of up to 300GB.
	* If you wish to save detailed logs of jobs you may want to append the following to your cluster creation command:
			```
	                --properties dataproc:dataproc.logging.stackdriver.enable=true \
			--properties dataproc:dataproc.logging.stackdriver.job.driver.enable=true \
			--properties dataproc:dataproc.logging.stackdriver.job.yarn.container.enable=true \
			--properties dataproc:jobs.file-backed-output.enable=true \
			--properties yarn:yarn.log-aggregation-enable=true \
			--properties yarn:yarn.nodemanager.remote-app-log-dir=gs://${BUCKET}/logs \
			--properties yarn:yarn.log-aggregation.retain-seconds=-1 \
	                ```

**Reconfigurable variables**
1. Datasets GCS bucket location: 'datasets_bucket'
2. Datasets folder location within GCS bucket: 'datasets_folder'
3. Dataset sizes (also the name of each dataset [3GB, 20GB, etc.]): 'dataset_sizes'
4. Spark DataSource format (only change this if you know what you're doing or if the Spark DataSource name was updated): 'ds_format'
5. The number of tests to run for each size: num_tests_for_each
6. Repartition to the max stream quota, or not? :  repartition_mode
7. The quota for the maximum number of streams (if in repartition_mode): max_stream_quota

	Here are the reconfigurable Spark options for each dataset size (set these up below in  the writing loop, for each size. The default for each option is the empty string ''):
8. 'executors': the number of executors to allocate for each dataset size batch-writing Reconfigures the Spark 'spark.dynamicAllocation.maxExecutors' option. When 'executors' is specified, the 'spark.dynamicAllocation.enabled' is set to True automatically. Refer to https://spark.apache.org/docs/2.4.5/configuration.html .
9. 'executorMemory' = the memory allocated to each executor for a specific dataset size batch-write. Reconfigures the Spark 'spark.executor.memory' option. Refer to https://spark.apache.org/docs/2.4.5/configuration.html .

			All 9 variables above can be found in the file vortex.py with the RECONFIGURABLE keyword (in a comment above / below). They can be changed up to your discretion.
			However, the default values are as follows:
1. datasets_bucket = '' *This variable must be changed and cannot be empty. Please create / retrieve your Google Cloud Storage bucket name, and replace this variable with the string representation of that name. This variable may be found on line 53 of the file vortex.py*
2. datasets_folder = 'datasets'
3. dataset_sizes = [3, 20, 100, 300, 1000]
4. ds_format = "com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2"
5. num_tests_for_each = 10
6. repartition_mode = True
7. max_stream_quota = 100
	Variables 8 and 9 may be reconfigured for each data size (in GB):
8. executors:
	* for size = 3, executors = '2'
	* for size = 20, executors = '4'
	* for size = 100, executors = '40'
	* for size = 300, executors = '120'
	* for size = 1000, executors = '250'
9. executorMemory:
	* for size = 3, executorMemory = ''
	* for size = 20, executorMemory = ''
	* for size = 100, executorMemory = ''
	* for size = 300, executorMemory = ''
	* for size = 1000, executorMemory = '17g' *The g here signifies 17GB of memory per executor*
	For more information about how 'executors' and 'executorMemory' are set, refer to https://spark.apache.org/docs/2.4.5/configuration.html and see above about which Spark Configuration options are set for each variable.



**Notes:**
* because this test recreates the SparkSession for every different size data set, the Spark UI on Dataproc will show the tests for each size in multiple separate Spark applications.
* this stress test is meant to be as reconfigurable as possible, as such there are some variables that can be reconfigured below.
