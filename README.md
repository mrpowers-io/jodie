# jodie

This library provides helpful Delta Lake and filesystem utility functions.

![jodie](https://github.com/MrPowers/jodie/blob/main/images/jodie.jpeg)

## Accessing the library

*How to access the code*

## Delta

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
Type2Scd.upsert(path, updatesDF, "pkey", Seq("attr1", "attr2"))
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

### Remove Duplicates

There are two versions of the `removeDuplicateRecords` function. One deletes all the duplicated records from a table, and
the other one also deletes duplicates but keeps one occurrence of each record that was duplicated.

#### Let’s see an example of how to use the first version:

Suppose you have the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+-----------+
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
DeltaHelpers.removeDuplicateRecords(deltaTable = deltaTable, duplicateColumns = Seq("firstname","lastname"))
```

The result of running the previous function is the following table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+-----------+
|   2|    Maria|   Willis|
|   6|    Maria|     Pitt|
+----+---------+---------+
```

#### Now let’s see an example of how to use the second version:

Suppose you have the same initial table:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+-----------+
|   1|   Benito|  Jackson| # duplicate
|   2|    Maria|   Willis|
|   3|     Jose| Travolta| # duplicate
|   4|   Benito|  Jackson| # duplicate
|   5|     Jose| Travolta| # duplicate
|   6|    Maria|     Pitt|
|   9|   Benito|  Jackson| # duplicate
+----+---------+---------+
```

We can Run the following function to remove duplicates but keep one occurrence of each record that was duplicated:

```scala
DeltaHelpers.removeDuplicateRecords(deltaTable = deltaTable, primaryKey = "id", duplicateColumns = Seq("firstname","lastname"))
```

The result of running the previous function is the following:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+-----------+
|   1|   Benito|  Jackson|
|   2|    Maria|   Willis|
|   3|     Jose| Travolta|
|   6|    Maria|     Pitt|
+----+---------+---------+
```

Note how we keep one occurrence of each record that was duplicated. This function comes in handy when you are doing data
cleansing.

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
+----+---------+-----------+
|   1|   Benito|  Jackson|
|   4|    Maria|     Pitt|
|   6|  Rosalia|     Pitt|
+----+---------+---------+
```
And we want to insert this new dataframe:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+-----------+
|   6|  Rosalia|     Pitt| # duplicate
|   2|    Maria|   Willis|
|   3|     Jose| Travolta|
|   4|    Maria|     Pitt| # duplicate
+----+---------+---------+
```

We can use the following function to insert new data and avoid data duplication:
```scala
DeltaHelpers.appendWithoutDuplicates(deltaTable = deltaTable,appendData = newDataDF, primaryKeysColumns = Seq("firstname","lastname"))
```

The result table will be the following:

```
+----+---------+---------+
|  id|firstname| lastname|
+----+---------+-----------+
|   1|   Benito|  Jackson|
|   4|    Maria|     Pitt|
|   6|  Rosalia|     Pitt|
|   2|    Maria|   Willis|
|   3|     Jose| Travolta| 
+----+---------+---------+
```

## Hive
### Create View
This function `createOrReplaceHiveView` creates a hive view from a delta table. The View will contain all the columns
of the delta table, meaning that it will be like coping the table to a view not filtering or transformations are possible.

Here's how to use the function:
```scala
HiveHelpers.createOrReplaceHiveView(viewName = "students",deltaPath = "file:/path/to/your/delta-lake/table",deltaVersion = 100L)
```

Note that this function will create the hive view based on a specific version of the delta table. 

### Get Table Type
The function `getTableType` return the table type(Managed, External or Non-registered) of a given table name. The
return type is a enum value containing the label string.

Here's how to use the function:
```scala
HiveHelpers.getTableType(tableName = "students")
```
The result will be an HiveTableType:

```scala
HiveTableType.EXTERNAL(label = "EXTERNAL")
```

### Register a Delta table to Hive
The function `registerTable` adds metadata information of a delta table to the Hive metastore, 
this enables it to be queried.

Here is how to use the function:
```scala
HiveHelpers.registerTable(tableName = "students",tableLoc = "file:/path/to/your/delta-lake/table")
```
after that you would be able to query, i.e:
```scala
SparkSession.active.sql("select * from students").show
```

## HDFS Operations
### Configuration
for authentication, you can use environment variables or provide an xml config file:
* Config File
  * To set a configuration file you should follow the official hadoop documentation
    * [Hadoop General](https://hadoop.apache.org/docs/r3.2.1/hadoop-project-dist/hadoop-common/AdminCompatibilityGuide.html#XML_Configuration_Files)  
    * [AWS](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Authentication_properties)
    * [AZURE](https://hadoop.apache.org/docs/stable/hadoop-azure/index.html#Configuring_Credentials)
  * And set the environment variable **CONFIG_PATH** with the file path where the configuration file is stored
  * S3 Example: 
    ```xml
    <configuration>
      <property>
          <name>fs.s3a.access.key</name>
          <value>AWS access key ID</value>
      </property>
      <property>
          <name>fs.s3a.secret.key</name>
          <value>AWS secret key</value>
      </property>
    </configuration>
    ```
* Environment Variables
  * AWS
    * AWS_ACCESS_KEY_ID
    * AWS_SECRET_ACCESS_KEY
  * Azure Object Store / Data lake
    * AZURE_TENANT_ID
    * AZURE_CLIENT_ID
    * AZURE_CLIENT_SECRET

### Create Folder
This function creates a folder in a filesystem/object store based on a given name, the folder name it 
should follow the rules of the provider.

```scala
val root = hio.Path("s3a://bucket_name")
hio.mkdir(root / "path/to/folder")
```

### List Files/Folders
The function search in the given folder and returns the paths of every file or folder within it. 
It also supports searching given a wildcard. 

```scala
val wd = hio.Path("s3a://bucket_name/path/to/folders")
hio.ls(wd)
```
or to search given a wildcard
```scala
val wd = hio.Path("s3a://bucket_name/path/to/folders")
hio.ls.withWildCard(wd / "*.txt")
```

The execution of the function will return an array of string containing the paths as follows:
```scala
ArraySeq(
  "s3a://bucket_name/path/to/folders/file_1.txt", 
  "s3a://bucket_name/path/to/folders/file_2.txt", 
  "s3a://bucket_name/path/to/folders/new_folder")
```

Note that if you use the wildcard function with only a directory you will not get all the files and folders within it,
instead it will return only the given folder. e.g: 

```scala
val wd = hio.Path("s3a://bucket_name/path/to/folders")
hio.ls.withWildCard(wd)
```

returns 

```scala
ArraySeq("s3a://bucket_name/path/to/folders")
```


### Delete Files/Folders 
The function `remove` permanently deletes files or folders from a filesystem/object store. It is also possible 
to recursively deletes sub-folders/files using `remove.all`. e.g:

```scala
val filePath = hio.Path("s3a://bucket_name/path/to/file")
hio.remove(filePath)
```
or recursively deletes
```scala
val folderPath = hio.Path("s3a://bucket_name/path/to/folder")
hio.remove.all(folderPath)
```

### Copy Files
The function `copy` creates a copy of the files in the source folder in the destination folder. It is also possible 
to use this function with wild card `copy.withWildCard`. e.g:

```scala
val src = hio.Path("s3a://bucket_name/path/to/src")
val dest = hio.Path("s3a://bucket_name/path/to/dest")
hio.copy(src,dest)
```
or use it with wildcard
```scala
val src = hio.Path("s3a://bucket_name/path/to/src/*.parquet")
val dest = hio.Path("s3a://bucket_name/path/to/dest")
hio.copy.withWildCard(src,dest)
```

### Move Files
The function `move` creates a copy of the files in the source folder in the destination folder and remove the files 
from the source folder. It is also possible to use this function with wild card `move.withWildCard`. e.g:

```scala
val src = hio.Path("s3a://bucket_name/path/to/src")
val dest = hio.Path("s3a://bucket_name/path/to/dest")
hio.move(src,dest)
```
or use it with wildcard
```scala
val src = hio.Path("s3a://bucket_name/path/to/src/*.parquet")
val dest = hio.Path("s3a://bucket_name/path/to/dest")
hio.move.withWildCard(src,dest)
```

### Create Files
This function `write` creates a file in a filesystem from an array of bytes or a string. To create the file
the folder must exist.

```scala
val fileContentInStr =
  """
    |name,lastname,age
    |Maria,Willis,36
    |Benito,Jackson,28
    |""".stripMargin
val wd = hio.Path("s3a://bucket_name/path/to/folders/data.csv")
hio.write(wd,fileContentInStr)
```

### Read Files
This function reads a file from the filesystem/object store and return its representation in
byte array or string.

```scala
val wd = hio.Path("s3a://bucket_name/path/to/folders")
hio.read(wd / "file_1.txt")
```
or to automatically parse to string
```scala
val wd = hio.Path("s3a://bucket_name/path/to/folders")
hio.read.string(wd / "file_1.txt")
```

## More about Jodie

See [this video](https://www.youtube.com/watch?v=llHKvaV0scQ) for more info about the awesomeness of Jodie!
