# MapReduceJoinExample
An example of a reduce join implementation

# Run example
hadoop jar MapReduceJoinExample-1.0-SNAPSHOT-job.jar s3://dags-public/wikistats/ s3://dags-public/dbpedia/ /intermediate-`date +%Y-%m-%d-%H-%M-%S`/ /out-`date +%Y-%m-%d-%H-%M-%S`/