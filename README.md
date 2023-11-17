# jodie

This library provides helpful Delta Lake and filesystem utility functions.

![jodie](images/jodie.jpeg)

## Accessing the library

Fetch the JAR file from Maven.

```scala
libraryDependencies += "com.github.mrpowers" %% "jodie" % "0.0.3"
```

You can find the spark-daria releases for different Scala versions:

* [Scala 2.12 versions here](https://repo1.maven.org/maven2/com/github/mrpowers/jodie_2.12/)
* [Scala 2.13 versions here](https://repo1.maven.org/maven2/com/github/mrpowers/jodie_2.13/)

## Delta Helpers

### Type 2 SCDs

This library provides an opinionated, conventions over configuration, approach to Type 2 SCD management.  Let's look at an example before covering the conventions required to take advantage of the functionality.

Suppose you have the following SCD table with the `pkey` primary key:

```
+----+-----+-----+----------+-------------------+--------+
|pkey|attr1|attr2|is_current|     effective_time|end_time|
+----+-----+-----+----------+-------------------+--------+
|   1|    A|    A|      true|2019-01-01 00:00:00|    null|
|   2|    B|    B|      true|2019-01-01 00:00:00|    null|
|   4|    D|    D|      true|2019-01-01 00:00:00|    null|
+----+-----+-----+----------+-------------------+--------+
```

You'd like to perform an upsert with this data:

```
+----+-----+-----+-------------------+
|pkey|attr1|attr2|     effective_time|
+----+-----+-----+-------------------+
|   2|    Z| null|2020-01-01 00:00:00| // upsert data
|   3|    C|    C|2020-09-15 00:00:00| // new pkey
+----+-----+-----+-------------------+
```

Here's how to perform the upsert:

```scala
Type2Scd.upsert(deltaTable, updatesDF, "pkey", Seq("attr1", "attr2"))
```

Here's the table after the upsert:

```
+----+-----+-----+----------+-------------------+-------------------+
|pkey|attr1|attr2|is_current|     effective_time|           end_time|
+----+-----+-----+----------+-------------------+-------------------+
|   2|    B|    B|     false|2019-01-01 00:00:00|2020-01-01 00:00:00|
|   4|    D|    D|      true|2019-01-01 00:00:00|               null|
|   1|    A|    A|      true|2019-01-01 00:00:00|               null|
|   3|    C|    C|      true|2020-09-15 00:00:00|               null|
|   2|    Z| null|      true|2020-01-01 00:00:00|               null|
+----+-----+-----+----------+-------------------+-------------------+
```

You can leverage the upsert code if your SCD table meets these requirements:

* Contains a unique primary key column
* Any change in an attribute column triggers an upsert
* SCD logic is exposed via `effective_time`, `end_time` and `is_current` column

`merge` logic can get really messy, so it's easiest to follow these conventions.  See [this blog post](https://mungingdata.com/delta-lake/type-2-scd-upserts/) if you'd like to build a SCD with custom logic.

### Kill Duplicates

The function `killDuplicateRecords` deletes all the duplicated records from a table given a set of columns.

Suppose you have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson| # duplicate
|   2|    Maria|   Willis|
|   3|     Jose| Travolta| # duplicate
|   4|   Benito|  Jackson| # duplicate
|   5|     Jose| Travolta| # duplicate
|   6|    Maria|     Pitt|
|   9|   Benito|  Jackson| # duplicate
+----+---------+---------+
```

We can Run the following function to remove all duplicates:

```scala
DeltaHelpers.killDuplicateRecords(
  deltaTable = deltaTable, 
  duplicateColumns = Seq("firstname","lastname")
)
```

The result of running the previous function is the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   2|    Maria|   Willis|
|   6|    Maria|     Pitt| 
+----+---------+---------+
```

### Remove Duplicates

The functions `removeDuplicateRecords` deletes duplicates but keeps one occurrence of each record that was duplicated.
There are two versions of that function, lets look an example of each,

#### Let’s see an example of how to use the first version:

Suppose you have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   2|    Maria|   Willis|
|   3|     Jose| Travolta|
|   4|   Benito|  Jackson|
|   1|   Benito|  Jackson| # duplicate
|   5|     Jose| Travolta| # duplicate
|   6|    Maria|   Willis| # duplicate
|   9|   Benito|  Jackson| # duplicate
+----+---------+---------+
```
We can Run the following function to remove all duplicates:

```scala
DeltaHelpers.removeDuplicateRecords(
  deltaTable = deltaTable,
  duplicateColumns = Seq("firstname","lastname")
)
```

The result of running the previous function is the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   2|    Maria|   Willis|
|   3|     Jose| Travolta|
|   4|   Benito|  Jackson| 
+----+---------+---------+
```

#### Now let’s see an example of how to use the second version:

Suppose you have a similar table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   2|    Maria|   Willis|
|   3|     Jose| Travolta| # duplicate
|   4|   Benito|  Jackson| # duplicate
|   1|   Benito|  Jackson| # duplicate
|   5|     Jose| Travolta| # duplicate
|   6|    Maria|     Pitt|
|   9|   Benito|  Jackson| # duplicate
+----+---------+---------+
```

This time the function takes an additional input parameter, a primary key that will be used to sort 
the duplicated records in ascending order and remove them according to that order.

```scala
DeltaHelpers.removeDuplicateRecords(
  deltaTable = deltaTable,
  primaryKey = "id",
  duplicateColumns = Seq("firstname","lastname")
)
```

The result of running the previous function is the following:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson|
|   2|    Maria|   Willis|
|   3|     Jose| Travolta|
|   6|    Maria|     Pitt|
+----+---------+---------+
```

These functions come in handy when you are doing data cleansing.

### Copy Delta Table

This function takes an existing delta table and makes a copy of all its data, properties,
and partitions to a new delta table. The new table could be created based on a specified path or
just a given table name. 

Copying does not include the delta log, which means that you will not be able to restore the new table to an old version of the original table.

Here's how to perform the copy to a specific path:

```scala
DeltaHelpers.copyTable(deltaTable = deltaTable, targetPath = Some(targetPath))
```

Here's how to perform the copy using a table name:

```scala
DeltaHelpers.copyTable(deltaTable = deltaTable, targetTableName = Some(tableName))
```

Note the location where the table will be stored in this last function call 
will be based on the spark conf property `spark.sql.warehouse.dir`.

### Validate append

The `validateAppend` function provides a mechanism for allowing some columns for schema evolution, but rejecting appends with columns that aren't specificly allowlisted.

Suppose you have the following Delta table:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   2|   b|   B|
|   1|   a|   A|
+----+----+----+
```
Here's an appender function that wraps `validateAppend`:

```scala
DeltaHelpers.validateAppend(
  deltaTable = deltaTable,
  appendDF = appendDf,
  requiredCols = List("col1", "col2"),
  optionalCols = List("col4")
)
```

You can append the following DataFrame that contains the required columns and the optional columns:

```
+----+----+----+
|col1|col2|col4|
+----+----+----+
|   3|   c| cat|
|   4|   d| dog|
+----+----+----+
```

Here's what the Delta table will contain after that data is appended:

```
+----+----+----+----+
|col1|col2|col3|col4|
+----+----+----+----+
|   3|   c|null| cat|
|   4|   d|null| dog|
|   2|   b|   B|null|
|   1|   a|   A|null|
+----+----+----+----+
```

You cannot append the following DataFrame which contains the required columns, but also contains another column (`col5`) that's not specified as an optional column.

```
+----+----+----+
|col1|col2|col5|
+----+----+----+
|   4|   b|   A|
|   5|   y|   C|
|   6|   z|   D|
+----+----+----+
```

Here's the error you'll get when you attempt this write: "The following columns are not part of the current Delta table. If you want to add these columns to the table, you must set the optionalCols parameter: List(col5)"

You also cannot append the following DataFrame which is missing one of the required columns.

```
+----+----+
|col1|col4|
+----+----+
|   4|   A|
|   5|   C|
|   6|   D|
+----+----+
```

Here's the error you'll get: "The base Delta table has these columns List(col1, col4), but these columns are required List(col1, col2)"


### Latest Version of Delta Table
The function `latestVersion` return the latest version number of a table given its storage path. 

Here's how to use the function:
```scala
DeltaHelpers.latestVersion(path = "file:/path/to/your/delta-lake/table")
```

### Insert Data Without Duplicates
The function `appendWithoutDuplicates` inserts data into an existing delta table and prevents data duplication in the process.
Let's see an example of how it works.

Suppose we have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson|
|   4|    Maria|     Pitt|
|   6|  Rosalia|     Pitt|
+----+---------+---------+
```
And we want to insert this new dataframe:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   6|  Rosalia|     Pitt| # duplicate
|   2|    Maria|   Willis|
|   3|     Jose| Travolta|
|   4|    Maria|     Pitt| # duplicate
+----+---------+---------+
```

We can use the following function to insert new data and avoid data duplication:
```scala
DeltaHelpers.appendWithoutDuplicates(
  deltaTable = deltaTable,
  appendData = newDataDF, 
  compositeKey = Seq("firstname","lastname")
)
```

The result table will be the following:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson|
|   4|    Maria|     Pitt|
|   6|  Rosalia|     Pitt|
|   2|    Maria|   Willis|
|   3|     Jose| Travolta| 
+----+---------+---------+
```
### Generate MD5 from columns

The function `withMD5Columns` appends a md5 hash of specified columns to the DataFrame. This can be used as a unique key 
if the selected columns form a composite key. Here is an example

Suppose we have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson|
|   4|    Maria|     Pitt|
|   6|  Rosalia|     Pitt|
+----+---------+---------+
```

We use the function in this way:
```scala
DeltaHelpers.withMD5Columns(
  dataFrame = inputDF,
  cols = List("firstname","lastname"),
  newColName = "unique_id")
)
```

The result table will be the following:
```
+----+---------+---------+----------------------------------+
|  id|firstname| lastname| unique_id                        |
+----+---------+---------+----------------------------------+
|   1|   Benito|  Jackson| 3456d6842080e8188b35f515254fece8 |
|   4|    Maria|     Pitt| 4fd906b56cc15ca517c554b215597ea1 |
|   6|  Rosalia|     Pitt| 3b3814001b13695931b6df8670172f91 |
+----+---------+---------+----------------------------------+
```

You can use this function with the columns identified in findCompositeKeyCandidate to append a unique key to the DataFrame.

### Find Composite Key
This function `findCompositeKeyCandidate` helps you find a composite key that uniquely identifies the rows your Delta table. 
It returns a list of columns that can be used as a composite key. i.e:

Suppose we have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson|
|   4|    Maria|     Pitt|
|   6|  Rosalia|     Pitt|
+----+---------+---------+
```

Now execute the function:
```scala
val result = DeltaHelpers.findCompositeKeyCandidate(
  deltaTable = deltaTable,
  excludeCols = Seq("id")
)
```

The result will be the following:

```scala
Seq("firstname","lastname")
```

### Validate Composite Key
The  `isCompositeKeyCandidate` function aids in verifying whether a given composite key qualifies as a unique key within your Delta table. 
It returns true if the key is considered a potential composite key, and false otherwise.

Suppose we have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+---------+
|   1|   Benito|  Jackson|
|   4|    Maria|     Pitt|
|   6|  Rosalia| Travolta|
+----+---------+---------+
```

Now execute the function:
```scala
val result = DeltaHelpers.isCompositeKeyCandidate(
  deltaTable = deltaTable,
  cols = Seq("id", "firstName")
)
```

The result will be the following:

```scala
true
```


## Delta File Sizes

The `deltaFileSizes` function returns a `Map[String,Long]` that contains the total size in bytes, the amount of files and the
average file size for a given Delta Table.

Suppose you have the following Delta Table, partitioned by `col1`:

```
+----+----+----+
|col1|col2|col3|
+----+----+----+
|   1|   A|   A|
|   2|   A|   B|
+----+----+----+
```

Running `DeltaHelpers.deltaFileSizes(deltaTable)` on that table will return:

```scala
Map("size_in_bytes" -> 1320,
  "number_of_files" -> 2,
  "average_file_size_in_bytes" -> 660)
```

## Delta Table File Size Distribution
The function `deltaFileSizeDistributionInMB` returns a `DataFrame` that contains the following stats in megabytes about file sizes in a Delta Table:
### `No. of Parquet Files, Mean File Size, Standard Deviation, Minimum File Size, Maximum File Size, 10th Percentile, 25th Percentile, Median, 75th Percentile, 90th Percentile, 95th Percentile.`

This function also works on partition condition. For example, if you have a Delta Table partitioned by `country` and you want to know the file size distribution for `country = 'Australia''`, you can run the following:
```scala
DeltaHelpers.deltaFileSizeDistribution(path, Some("country='Australia'"))
```
This will return a `DataFrame` with the following columns:
```scala
+------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+-----------------------------------------------------------------------------------------------------------------------+
|partitionValues                                 |num_of_parquet_files|mean_size_of_files|stddev              |min_file_size       |max_file_size     |Percentile[10th, 25th, Median, 75th, 90th, 95th]                                                                       |
+------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+-----------------------------------------------------------------------------------------------------------------------+
|[{country, Australia}]                          |1429                |30.205616120778238|0.3454942220373272  |17.376179695129395  |30.377344131469727|[30.132079124450684, 30.173019409179688, 30.215540885925293, 30.25797176361084, 30.294878005981445, 30.318415641784668]|
+------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+-----------------------------------------------------------------------------------------------------------------------+
```
Generally, if no partition condition is provided, the function will return the `file size distribution` for the whole Delta Table (with or without partition wise).
```scala
DeltaHelpers.deltaFileSizeDistribution(path)

+------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+-----------------------------------------------------------------------------------------------------------------------+
|partitionValues                                 |num_of_parquet_files|mean_size_of_files|stddev              |min_file_size       |max_file_size     |Percentile[10th, 25th, Median, 75th, 90th, 95th]                                                                       |
+------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+-----------------------------------------------------------------------------------------------------------------------+
|[{country, Mauritius}]                          |2502                |28.14731636093103 |0.7981461034111957  |0.005436897277832031|28.37139320373535 |[28.098042488098145, 28.12824249267578, 28.167524337768555, 28.207666397094727, 28.246790885925293, 28.265881538391113]|
|[{country, Malaysia}]                           |3334                |34.471798611888644|0.4018671378261647  |11.515838623046875  |34.700727462768555|[34.40602779388428, 34.43935298919678, 34.47779560089111, 34.51614856719971, 34.55129528045654, 34.57488822937012]     |
|[{country, GrandDuchyofLuxembourg}]             |808                 |2.84647535569597  |0.5369371124495063  |0.006397247314453125|3.0397253036499023|[2.8616743087768555, 2.8840208053588867, 2.9723005294799805, 2.992110252380371, 3.0045957565307617, 3.0115060806274414]|
|[{country, Argentina}]                          |3372                |36.82978148392511 |5.336511210904255   |0.010506629943847656|99.95287132263184 |[36.29576301574707, 36.33060932159424, 36.369083404541016, 36.406826972961426, 36.442559242248535, 36.4655065536499]   |
|[{country, Australia}]                          |1429                |30.205616120778238|0.3454942220373272  |17.376179695129395  |30.377344131469727|[30.132079124450684, 30.173019409179688, 30.215540885925293, 30.25797176361084, 30.294878005981445, 30.318415641784668]|
+------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+-----------------------------------------------------------------------------------------------------------------------+
```
A similar function `deltaFileSizeDistribution` is provided which returns the same stats in bytes.
## Delta Table Number of Records Distribution
The function `deltaNumRecordDistribution` returns a `DataFrame` that contains the following stats about number of records in parquet files in a Delta Table:
### `No. of Parquet Files, Mean Num Records, Standard Deviation, Minimum & Maximum Number of Records in a File, 10th Percentile, 25th Percentile, Median, 75th Percentile, 90th Percentile, 95th Percentile.`

This function also works on partition condition. For example, if you have a Delta Table partitioned by `country` and you want to know the numRecords distribution for `country = 'Australia''`, you can run the following:
```scala
DeltaHelpers.deltaNumRecordDistribution(path, Some("country='Australia'"))
```
This will return a `DataFrame` with the following columns:
```scala
+------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+---------------------------------------------------------+
|partitionValues                                 |num_of_parquet_files|mean_num_records_in_files|stddev            |min_num_records|max_num_records|Percentile[10th, 25th, Median, 75th, 90th, 95th]            |
+------------------------------------------------+--------------------+-------------------------+------------------+---------------+---------------+------------------------------------------------------------+
|[{country, Australia}]                          |1429                |354160.2757172848        |4075.503669047513 |201823.0       |355980.0       |[353490.0, 353907.0, 354262.0, 354661.0, 355024.0, 355246.0]|
+------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+---------------------------------------------------------+
```
Generally, if no partition condition is provided, the function will return the `number of records distribution` for the whole Delta Table (with or without partition wise).
```scala
DeltaHelpers.deltaNumRecordDistribution(path)

+------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+---------------------------------------------------------+
|partitionValues                                 |num_of_parquet_files|mean_num_records_in_files|stddev            |min_num_records|max_num_records|Percentile[10th, 25th, Median, 75th, 90th, 95th]            |
+------------------------------------------------+--------------------+-------------------------+------------------+---------------+---------------+------------------------------------------------------------+
|[{country, Mauritius}]                          |2502                |433464.051558753         |12279.532110752265|1.0            |436195.0       |[432963.0, 433373.0, 433811.0, 434265.0, 434633.0, 434853.0]|
|[{country, Malaysia}]                           |3334                |411151.4946010798        |4797.137407595447 |136777.0       |413581.0       |[410390.0, 410794.0, 411234.0, 411674.0, 412063.0, 412309.0]|
|[{country, GrandDuchyofLuxembourg}]             |808                 |26462.003712871287       |5003.8118076056935|6.0            |28256.0        |[26605.0, 26811.0, 27635.0, 27822.0, 27937.0, 28002.0]      |
|[{country, Argentina}]                          |3372                |461765.5604982206        |79874.3727926887  |61.0           |1403964.0      |[453782.0, 454174.0, 454646.0, 455103.0, 455543.0, 455818.0]|
|[{country, Australia}]                          |1429                |354160.2757172848        |4075.503669047513 |201823.0       |355980.0       |[353490.0, 353907.0, 354262.0, 354661.0, 355024.0, 355246.0]|
+------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+---------------------------------------------------------+
```

## Number of Shuffle Files in Merge & Other Filter Conditions

The function `getNumShuffleFiles` gets the number of shuffle files (think of part files in parquet) that will be pulled into memory for a given filter condition. This is particularly useful to estimate memory requirements in a Delta Merge operation where the number of shuffle files can be a bottleneck. 
To better tune your jobs, you can use this function to get the number of shuffle files for different kinds of filter condition and then perform operations like merge, zorder, compaction etc. to see if you reach the desired no. of shuffle files.


For example, if the condition is "country = 'GBR' and age >= 30 and age <= 40 and firstname like '%Jo%' " and country is the partition column, 
```scala
DeltaHelpers.getNumShuffleFiles(path, "country = 'GBR' and age >= 30 and age <= 40 and firstname like '%Jo%' ")
```

then the output might look like following (explaining different parts of the condition as a key in the `Map` and the value contains the file count)
```scala
Map(
  // number of files that will be pulled into memory for the entire provided condition
  "OVERALL RESOLVED CONDITION => [ (country = 'GBR') and (age >= 30) and" +
  " (age = 40) and firstname LIKE '%Joh%' ]" -> 18,
  // number of files signifying the greater than/less than part => "age >= 30 and age <= 40"
  "GREATER THAN / LESS THAN PART => [ (age >= 30) and (age = 40) ]" -> 100,
  // number of files signifying the equals part => "country = 'GBR'
  "EQUALS/EQUALS NULL SAFE PART => [ (country = 'GBR') ]" -> 300,
  // number of files signifying the like (or any other) part => "firstname like '%Jo%' "
  "LEFT OVER PART => [ firstname LIKE '%Joh%' ]" -> 600,
  // number of files signifying any other part. This is mostly a failsafe
  //   1. to capture any other condition that might have been missed
  //   2. If wrong attribute names or conditions are provided like snapshot.id = source.id (usually found in merge conditions)
  "UNRESOLVED PART => [ (snapshot.id = update.id) ]" -> 800,
  // Total no. of files in the Delta Table
  "TOTAL_NUM_FILES_IN_DELTA_TABLE =>" -> 800,
  // List of unresolved columns/attributes in the provided condition. 
  // Will be empty if all columns are resolved.
  "UNRESOLVED_COLUMNS =>" -> List())
```

Another important use case this method can help with is to see the min-max range overlap. Adding a min max on a high cardinality column like id say `id >= 900 and id <= 5000` can actually help in reducing the no. of shuffle files delta lake pulls into memory. However, such a operation is not always guaranteed to work and the effect can be viewed when you run this method.

This function works only on the Delta Log and does not scan any data in the Delta Table.

If you want more information about these individual files and their metadata, consider using the `getShuffleFileMetadata` function.
## Change Data Feed Helpers

### CASE I - When Delta aka Transaction Log gets purged

`getVersionsForAvailableDeltaLog` - helps you find the versions within the `[startingVersion,endingVersion]`range for which Delta Log is present and CDF read is enabled (only for the start version) and possible
```scala
ChangeDataFeedHelper(deltaPath, 0, 5).getVersionsForAvailableDeltaLog
```
The result will return the same versions `Some(0,5)` if Delta Logs are present. Otherwise, it will return say `Some(10,15)` - the earliest queryable start version and latest snapshot version as ending version. If at any point within versions it finds that EDR is disabled, it returns a `None`.

`readCDFIgnoreMissingDeltaLog` - Returns an Option of Spark Dataframe for all versions provided by the above method
```scala
ChangeDataFeedHelper(deltaPath, 11, 13).readCDFIgnoreMissingDeltaLog.get.show(false)

+---+------+---+----------------+---------------+-------------------+
|id |gender|age|_change_type    |_commit_version|_commit_timestamp  |
+---+------+---+----------------+---------------+-------------------+
|4  |Female|25 |update_preimage |11             |2023-03-13 14:21:58|
|4  |Other |45 |update_postimage|11             |2023-03-13 14:21:58|
|2  |Male  |45 |update_preimage |13             |2023-03-13 14:22:05|
|2  |Other |67 |update_postimage|13             |2023-03-13 14:22:05|
|2  |Other |67 |update_preimage |12             |2023-03-13 14:22:01|
|2  |Male  |45 |update_postimage|12             |2023-03-13 14:22:01|
+---+------+---+----------------+---------------+-------------------+
```
Resultant Dataframe is the same as the result of CDF Time Travel query
### CASE II - When CDC data gets purged in `_change_data` directory

`getVersionsForAvailableCDC` - helps you find the versions within the `[startingVersion,endingVersion]`range for which underlying CDC data is present under `_change_data` directory. Call this method when java.io.FileNotFoundException is encountered during time travel
```scala
ChangeDataFeedHelper(deltaPath, 0, 5).getVersionsForAvailableCDC
```
The result will return the same versions `Some(0,5)` if CDC data is present for the given versions under `_change_data` directory. Otherwise, it will return `Some(2,5)` - the earliest queryable start version for which CDC is present and given ending version. If no version is found that has CDC data available, it returns a `None`.
 

`readCDFIgnoreMissingCDC` - Returns an Option of Spark Dataframe for all versions provided by the above method
```scala
ChangeDataFeedHelper(deltaPath, 11, 13).readCDFIgnoreMissingCDC.show(false)

+---+------+---+----------------+---------------+-------------------+
|id |gender|age|_change_type    |_commit_version|_commit_timestamp  |
+---+------+---+----------------+---------------+-------------------+
|4  |Female|25 |update_preimage |11             |2023-03-13 14:21:58|
|4  |Other |45 |update_postimage|11             |2023-03-13 14:21:58|
|2  |Male  |45 |update_preimage |13             |2023-03-13 14:22:05|
|2  |Other |67 |update_postimage|13             |2023-03-13 14:22:05|
|2  |Other |67 |update_preimage |12             |2023-03-13 14:22:01|
|2  |Male  |45 |update_postimage|12             |2023-03-13 14:22:01|
+---+------+---+----------------+---------------+-------------------+
```
Resultant Dataframe is the same as the result of CDF Time Travel query

### CASE III - Enable-Disable-Re-enable CDF

`getRangesForCDFEnabledVersions`- Skip all versions for which CDF was disabled and get all ranges for which CDF was enabled and time travel is possible within a `[startingVersion,endingVersion]`range
```scala
 ChangeDataFeedHelper(writePath, 0, 30).getRangesForCDFEnabledVersions
```
The result will look like `List((0, 3), (7, 8), (12, 20))` signifying all version ranges for which CDF is enabled. The function `getRangesForCDFDisabledVersions` returns exactly same `List` but this time it returns disabled version ranges.

`readCDFIgnoreMissingRangesForEDR`- Returns an Option of unionised Spark Dataframe for all version ranges provided by the above method
```scala
 ChangeDataFeedHelper(writePath, 0, 30).readCDFIgnoreMissingRangesForEDR
+---+------+---+----------------+---------------+-------------------+
|id |gender|age|_change_type    |_commit_version|_commit_timestamp  |
+---+------+---+----------------+---------------+-------------------+
|2  |Male  |25 |update_preimage |2              |2023-03-13 14:40:48|
|2  |Male  |100|update_postimage|2              |2023-03-13 14:40:48|
|1  |Male  |25 |update_preimage |1              |2023-03-13 14:40:44|
|1  |Male  |35 |update_postimage|1              |2023-03-13 14:40:44|
|2  |Male  |100|update_preimage |3              |2023-03-13 14:40:52|
|2  |Male  |101|update_postimage|3              |2023-03-13 14:40:52|
|1  |Male  |25 |insert          |0              |2023-03-13 14:40:34|
|2  |Male  |25 |insert          |0              |2023-03-13 14:40:34|
|3  |Female|35 |insert          |0              |2023-03-13 14:40:34|
|2  |Male  |101|update_preimage |8              |2023-03-13 14:41:07|
|2  |Other |66 |update_postimage|8              |2023-03-13 14:41:07|
|2  |Other |66 |update_preimage |13             |2023-03-13 14:41:24|
|2  |Other |67 |update_postimage|13             |2023-03-13 14:41:24|
|2  |Other |67 |update_preimage |14             |2023-03-13 14:41:27|
|2  |Other |345|update_postimage|14             |2023-03-13 14:41:27|
|2  |Male  |100|update_preimage |20             |2023-03-13 14:41:46|
|2  |Male  |101|update_postimage|20             |2023-03-13 14:41:46|
|4  |Other |45 |update_preimage |15             |2023-03-13 14:41:30|
|4  |Female|678|update_postimage|15             |2023-03-13 14:41:30|
|1  |Other |55 |update_preimage |18             |2023-03-13 14:41:40|
|1  |Male  |35 |update_postimage|18             |2023-03-13 14:41:40|
|2  |Other |345|update_preimage |19             |2023-03-13 14:41:43|
|2  |Male  |100|update_postimage|19             |2023-03-13 14:41:43|
+---+------+---+----------------+---------------+-------------------+
```
Resultant Dataframe is the same as the result of CDF Time Travel query but this time it will only have CDC for enabled versions ignoring all versions for which CDC was disabled.
### Dry Run
`dryRun`- This method works as a fail-safe to see if there are any CDF-related issues. If it doesn't throw any errors, then you can be certain the above-mentioned issues do not occur in your Delta Table for the given versions. When it does, it throws either an AssertionError or an IllegalStateException with appropriate error message

`readCDF`- Plain old time travel query, this is literally the method definition, that's it
```scala
spark.read.format("delta").option("readChangeFeed","true").option("startingVersion",0).("endingVersion",20).load(gcs_path)
```
Pair `dryRun` with `readCDF` to detect any CDF errors in your Delta Table
```scala
ChangeDataFeedHelper(writePath, 9, 13).dryRun().readCDF
```
If no error found, it will return a similar Spark Dataframe with CDF between given versions. 

## Operation Metric Helpers

### Count Metrics on Delta Table between 2 versions
This function displays all count metric stored in the Delta Logs across versions for the entire Delta Table. It skips versions which do not record 
these count metrics and presents a unified view. It shows the growth of a Delta Table by providing the record counts - 
**deleted**, **updated** and **inserted** against a **version**. For a **merge** operation, we additionally have a source dataframe to tally
with as **source rows = (deleted + updated + inserted) rows**.  Please note that you need to have enough Driver Memory
for processing the Delta Logs at driver level.
```scala
OperationMetricHelper(path,0,6).getCountMetricsAsDF()
```
The result will be following:
```scala
+-------+-------+--------+-------+-----------+
|version|deleted|inserted|updated|source_rows|
+-------+-------+--------+-------+-----------+
|6      |0      |108     |0      |108        |
|5      |12     |0       |0      |0          |
|4      |0      |0       |300    |300        |
|3      |0      |100     |0      |100        |
|2      |0      |150     |190    |340        |
|1      |0      |0       |200    |200        |
|0      |0      |400     |0      |400        |
+-------+-------+--------+-------+-----------+
```
### Count Metrics at partition level of Delta Table
This function provides the same count metrics as the above function, but this time at a partition level. If operations 
like **MERGE, DELETE** and **UPDATE** are executed **at a partition level**, then this function can help in visualizing count 
metrics for such a partition. However, **it will not provide correct count metrics if these operations are performed 
across partitions**. This is because Delta Log does not store this information at a log level and hence, need to be 
implemented separately (we intend to take this up in future). Please note that you need to have enough Driver Memory
for processing the Delta Logs at driver level.
```scala
OperationMetricHelper(path).getCountMetricsAsDF(
  Some(" country = 'USA' and gender = 'Female'"))

// The same metric can be obtained generally without using spark dataframe
def getCountMetrics(partitionCondition: Option[String] = None)
                    : Seq[(Long, Long, Long, Long, Long)]
```
The result will be following:
```scala
+-------+-------+--------+--------+-----------+
|version|deleted|inserted| updated|source_rows|
+-------+-------+--------+--------+-----------+
|     27|      0|       0|20635530|   20635524|
|     14|      0|       0| 1429460|    1429460|
|     13|      0|       0| 4670450|    4670450|
|     12|      0|       0|20635530|   20635524|
|     11|      0|       0| 5181821|    5181821|
|     10|      0|       0| 1562046|    1562046|
|      9|      0|       0| 1562046|    1562046|
|      6|      0|       0|20635518|   20635512|
|      3|      0|       0| 5181821|    5181821|
|      0|      0|56287990|       0|   56287990|
+-------+-------+--------+--------+-----------+
```
Supported Partition condition types
```scala
// Single Partition
Some(" country = 'USA'")
// Multiple Partition with AND condition. OR is not supported.
Some(" country = 'USA' and gender = 'Female'")
// Without Single Quotes
Some(" country = USA and gender = Female")
```

## How to contribute
We welcome contributions to this project, to contribute checkout our [CONTRIBUTING.md](CONTRIBUTING.md) file.

## How to build the project

### pre-requisites
* SBT 1.8.2
* Java 8
* Scala 2.12.12

### Building

To compile, run
`sbt compile`

To test, run
`sbt test`

To generate artifacts, run
`sbt package`

## Project maintainers

* Matthew Powers aka [MrPowers](https://github.com/MrPowers)
* Brayan Jules aka [brayanjuls](https://github.com/brayanjuls)
* Joydeep Banik Roy aka [joydeepbroy-zeotap](https://github.com/joydeepbroy-zeotap)

## More about Jodie

See [this video](https://www.youtube.com/watch?v=llHKvaV0scQ) for more info about the awesomeness of Jodie!
