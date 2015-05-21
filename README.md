# MapReduceJoinExample
An example of a reduce join implementation

# Run example
hadoop jar MapReduceJoinExample-1.0-SNAPSHOT-job.jar s3://dags-public/wikistats/ s3://dags-public/dbpedia/ /intermediate-`date +%Y-%m-%d-%H-%M-%S`/ /out-`date +%Y-%m-%d-%H-%M-%S`/

# Details
It takes two data sets as parameters called wikistats and dbpedia

Wikistats has the following columns:
* Site
* Page
* Views
* Content length

DBPedia has the following:
* Page
* W3 url
* Category
* Flag

## Job1
* We select out page and views from wikistats and page and page and category from dbpedia. We use a composite key to tag the data set number with 0 for wikistats and 1 for dbpedia.
* Since we have a composite key from both data sets, a custom partitioner, partitioning on page is required.
* We implement a key class with a custom sort comparator and group comparator. 
* Sorting comparator ensures that the rows from wikistats appears before the ones from dbpedia.
* Grouping comparator ensures the same page is grouped together.
* The reduce phase simply sums number of views for rows coming from wikistats. For rows coming from dbpedia, we output the category and number of views which we sumed earlier.

## Job2
* We simply output the category and sum. 
* We use the built-in LongSumReducer to simply sum the different categories. 
* We leverage a combiner to perform some of the suming in the map phase before shuffle.