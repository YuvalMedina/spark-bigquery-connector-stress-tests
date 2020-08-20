# spark-bigquery-connector-stress-tests
Stress tests for the spark-bigquery-connector

**Instructions:**

This is a test to run 3, 20, 100, 300, and 1000GB batch writes on the Spark-BigQuery connector's writesupport integration.
			
			
This test is to be run with the following command:
		
		
```
gcloud dataproc jobs submit pyspark \
  --cluster=<YOUR-CLUSTER> \
  <LOCATION-OF-vortex.py> \
  --jars=${CONNECTOR} \
  -- \
    <YOUR_BUCKET> \
      [--datasets_folder=DATASET_FOLDER] [--dataset_sizes=DATASET_SIZES] \
      [--ds_format=DATASOURCE_FORMAT] [--num_tests_for_each=NUM_TESTS_FOR_EACH] \
      [--repartition_mode=REPARTITION_MODE] \
      [--max_stream_quota=MAX_BIGQUERY_STORAGE_WRITE_STREAM_QUOTA] \
      [--executors=EXECUTOR_NUMBER_LIST] [--executorMemory=EXECUTOR_MEMORY_LIST]
```
			

The location of the jar file above will change when the Spark <-> BigQuery WriteSupport integration is merged into the Spark Connector repository on GitHub, and is released.

**Before running this test:**

1. Please create / retrieve your Google Cloud Storage bucket name.
2. Then please copy the datasets for the stress test into your bucket using the command:

```
gcloud dataproc jobs submit pyspark \
  --cluster=<YOUR-CLUSTER> \
  <LOCATION-OF-save_datasets.py> \
  -- \
    <YOUR_BUCKET> \
    [--user_folder=CONTAINING_FOLDER]
```
* The optional `--user_folder` argument should be specified if you wish to place your datasets in a containing folder. Otherwise they will be placed in a defualt folder named `datasets`. Using the default option is recommended.

	* However, if you specified an inner directory in the `--user_folder` argument when copying the datasets, the folder `datasets` will be nested inside the directory you specified.
	* In this case, every time you run your stress test you will have to pass as an argument the location inside your bucket that you specified (such as `folder1/folder2/.../datasets`) into variable `--datasets_folder`.
		
3. Please create an appropriate cluster for testing.

* Your cluster must have pip packages `google-cloud-bigquery` and `google-cloud-storage` installed. A sample command to do this:
	
	```
	gcloud dataproc clusters create <YOUR-CLUSTER-NAME> \
	  --region=$REGION \
	  --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/python/pip-install.sh \
	  --metadata='PIP_PACKAGES=google-cloud-storage google-cloud-bigquery' \
	  --num-workers=<NUMBER-OF-WORKERS> \
	  --worker-machine-type=<WORKER-MACHINE-TYPE>
	```

* Please set your current Dataproc region using `export REGION=<DATAPROC_REGION>` before running this command.
* If you wish to run stress tests for up to 1000GB, the recommended number of workers is `250`, and the recommended machine type is `n2-standard-8`.
	* If your project doesn't have enough allowance for this, you may want to create your cluster with `120` workers, and use the standard machine type of `n1-standard-4`: with this, you may run write jobs of up to 300GB.
* If you wish to save detailed logs of jobs you may want to append the following to your cluster creation command:
			
```
--properties dataproc:dataproc.logging.stackdriver.enable=true \
  --properties dataproc:dataproc.logging.stackdriver.job.driver.enable=true \
  --properties dataproc:dataproc.logging.stackdriver.job.yarn.container.enable=true \
  --properties dataproc:jobs.file-backed-output.enable=true \
  --properties yarn:yarn.log-aggregation-enable=true \
  --properties yarn:yarn.nodemanager.remote-app-log-dir=gs://${BUCKET}/logs \
  --properties yarn:yarn.log-aggregation.retain-seconds=-1 
```

**Reconfigurable variables**
1. Datasets GCS bucket location:
	* passed as the first, required, extra argument (following the `--`) when running the stress test.
2. Datasets folder location within GCS bucket: `--datasets_folder`
3. Dataset sizes: `--dataset_sizes`
	* passed as multiple int's. Ex: `--dataset_sizes 3 20 100`
4. Spark DataSource format (only change this if you know what you're doing or if the Spark DataSource name was updated): `--ds_format`
5. The number of tests to run for each size: `--num_tests_for_each`
6. Repartition to the max stream quota, or not? :  `--repartition_mode`
7. The quota for the maximum number of streams (if in repartition_mode): `--max_stream_quota`

* There are also Spark options that are reconfigurable for each datasize
	* pass these as multiple values. 
		* (the number of values passed must equal the number of values passed to `--dataset_sizes`, or the default (`5`) number of sizes tested.)
	* if you wish to use the default value for a specific size, and to specify a value for another size you may type `null` for each datasize where you want to use the default option. Ex:
	```
	--executors 1 2 null
	```
	will specify that for 3GB we want to cap the number of maximum executors to 1, for 20GB to 2 executors, and for 100GB to use the default.
8. `--executors`: the maximum number of executors to allocate for each dataset size batch-writing.
	* Reconfigures the Spark `spark.dynamicAllocation.maxExecutors` option. 
	* When 'executors' is specified for a specific dataset-size, the `spark.dynamicAllocation.enabled` is set to `True` automatically.
	* Refer to https://spark.apache.org/docs/2.4.5/configuration.html .
9. `--executorMemory` = the memory allocated to each executor for a specific dataset size batch-write.
	* Reconfigures the Spark `spark.executor.memory` option.
	* Example use case:
	```
	--executorMemory 1k 10m null 5g 6t
	```
	will specify that for the 3GB dataset we want to allocate 1kB to each executor, for the 20GB dataset we allocate 10MB, for the 100GB dataset we use the default memory allocated, for the 300GB dataset we allocate 5GB, and for the 1000GB dataset we allocate 6TB.
	* Formatted as a number followed by a size unit suffix (`k`, `m`, `g` or `t`)
	* Refer to https://spark.apache.org/docs/2.4.5/configuration.html .



<!-- -->

All 9 variables above can be found in the file vortex.py with the RECONFIGURABLE keyword (in a comment above / below). They can be specified when running the stress test, and all are optional except the bucket keyword (for all optional values there is a default value).

The default values are as follows:
1. datasets_bucket -- NO DEFAULT
	* This variable must be changed and cannot be empty. Please create / retrieve your Google Cloud Storage bucket name, and set this variable to your bucket's name when running the test.
2. datasets_folder = `datasets`
3. dataset_sizes = `[3, 20, 100, 300, 1000]`
4. ds_format = `com.google.cloud.spark.bigquery.v2.BigQueryWriteSupportDataSourceV2`
5. num_tests_for_each = `10`
6. repartition_mode = `True`
7. max_stream_quota = `100`
	Variables 8 and 9 may be reconfigured for each data size:
8. executors:
	* for size = `3`GB, executors = `'2'`
	* for size = `20`GB, executors = `'4'`
	* for size = `100`GB, executors = `'40'`
	* for size = `300`GB, executors = `'120'`
	* for size = `1000`GB, executors = `'250'`
9. executorMemory:
	* for size = `3`GB, executorMemory = `''` (thus we use the cluster's default)
	* for size = `20`GB, executorMemory = `''` (cluster's default)
	* for size = `100`GB, executorMemory = `''` (cluster's default)
	* for size = `300`GB, executorMemory = `''` (cluster's default)
	* for size = `1000`GB, executorMemory = `'17g'` *The g here signifies 17GB of memory per executor*
	* For more information about how 'executors' and 'executorMemory' are set, refer to https://spark.apache.org/docs/2.4.5/configuration.html and see above about which Spark Configuration options are set for each variable.



**Notes:**
* because this test recreates the SparkSession for every different size data set, the Spark UI on Dataproc will show the tests for each size in multiple separate Spark applications.
	* This also means separate logs for each such application.
