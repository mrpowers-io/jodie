package mrpowers.jodie

import io.delta.tables._
import mrpowers.jodie.delta.DeltaConstants._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions._

import scala.collection.mutable

object DeltaHelpers {

  /**
   * Gets the latest version of a Delta lake
   */
  def latestVersion(path: String): Long =
    DeltaLog.forTable(SparkSession.active, path).snapshot.version


  /**
   * Gets the file size distribution in megabytes of a Delta table. Works at a partition level when partition
   * information is provided. Provides a more human readable version of the file size distribution. Provided columns are:
   * +------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+-----------------------------------------------------------------------------------------------------------------------+
   * |partitionValues                                 |num_of_parquet_files|mean_size_of_files|stddev              |min_file_size       |max_file_size     |Percentile[10th, 25th, Median, 75th, 90th, 95th]                                                                       |
   * +------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+-----------------------------------------------------------------------------------------------------------------------+
   * |[{country, Mauritius}]                          |2502                |28.14731636093103 |0.7981461034111957  |0.005436897277832031|28.37139320373535 |[28.098042488098145, 28.12824249267578, 28.167524337768555, 28.207666397094727, 28.246790885925293, 28.265881538391113]|
   * |[{country, Malaysia}]                           |3334                |34.471798611888644|0.4018671378261647  |11.515838623046875  |34.700727462768555|[34.40602779388428, 34.43935298919678, 34.47779560089111, 34.51614856719971, 34.55129528045654, 34.57488822937012]     |
   * |[{country, GrandDuchyofLuxembourg}]             |808                 |2.84647535569597  |0.5369371124495063  |0.006397247314453125|3.0397253036499023|[2.8616743087768555, 2.8840208053588867, 2.9723005294799805, 2.992110252380371, 3.0045957565307617, 3.0115060806274414]|
   * |[{country, Argentina}]                          |3372                |36.82978148392511 |5.336511210904255   |0.010506629943847656|99.95287132263184 |[36.29576301574707, 36.33060932159424, 36.369083404541016, 36.406826972961426, 36.442559242248535, 36.4655065536499]   |
   * |[{country, Australia}]                          |1429                |30.205616120778238|0.3454942220373272  |17.376179695129395  |30.377344131469727|[30.132079124450684, 30.173019409179688, 30.215540885925293, 30.25797176361084, 30.294878005981445, 30.318415641784668]|
   * +------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+-----------------------------------------------------------------------------------------------------------------------+
   *
   * @param path
   * @param condition
   * @return [[DataFrame]]
   */
  def deltaFileSizeDistributionInMB(path: String, condition: Option[String] = None): DataFrame =
    getAllPartitionStats(deltaFileStats(path, condition)
      .withColumn("size_in_mb", col(sizeColumn).divide(1024 * 1024)), statsPartitionColumn, "size_in_mb")
      .toDF(sizeDFColumns: _*)

  /**
   * Gets the file size distribution in bytes of a Delta table. Works at a partition level when partition
   * information is provided. Provided columns are same as [[deltaFileSizeDistributionInMB]]
   *
   * @param path
   * @param condition
   * @return [[DataFrame]]
   */
  def deltaFileSizeDistribution(path: String, condition: Option[String] = None): DataFrame =
    getAllPartitionStats(deltaFileStats(path, condition), statsPartitionColumn, sizeColumn).toDF(sizeDFColumns: _*)

  /**
   * Gets the file-wise number of records distribution of a Delta table. Works at a partition level when partition
   * information is provided. Provided columns are:
   * +------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+---------------------------------------------------------+
   * |partitionValues                                 |num_of_parquet_files|mean_num_records_in_files|stddev            |min_num_records|max_num_records|Percentile[10th, 25th, Median, 75th, 90th, 95th]            |
   * +------------------------------------------------+--------------------+-------------------------+------------------+---------------+---------------+------------------------------------------------------------+
   * |[{country, Mauritius}]                          |2502                |433464.051558753         |12279.532110752265|1.0            |436195.0       |[432963.0, 433373.0, 433811.0, 434265.0, 434633.0, 434853.0]|
   * |[{country, Malaysia}]                           |3334                |411151.4946010798        |4797.137407595447 |136777.0       |413581.0       |[410390.0, 410794.0, 411234.0, 411674.0, 412063.0, 412309.0]|
   * |[{country, GrandDuchyofLuxembourg}]             |808                 |26462.003712871287       |5003.8118076056935|6.0            |28256.0        |[26605.0, 26811.0, 27635.0, 27822.0, 27937.0, 28002.0]      |
   * |[{country, Argentina}]                          |3372                |461765.5604982206        |79874.3727926887  |61.0           |1403964.0      |[453782.0, 454174.0, 454646.0, 455103.0, 455543.0, 455818.0]|
   * |[{country, Australia}]                          |1429                |354160.2757172848        |4075.503669047513 |201823.0       |355980.0       |[353490.0, 353907.0, 354262.0, 354661.0, 355024.0, 355246.0]|
   * +------------------------------------------------+--------------------+------------------+--------------------+--------------------+------------------+---------------------------------------------------------+
   *
   * @param path
   * @param condition
   * @return [[DataFrame]]
   */
  def deltaNumRecordDistribution(path: String, condition: Option[String] = None): DataFrame =
    getAllPartitionStats(deltaFileStats(path, condition), statsPartitionColumn, numRecordsColumn).toDF(numRecordsDFColumns: _*)

  /**
   * Gets the number of shuffle files (part files for parquet) that will be pulled into memory for a given filter condition.
   * This is particularly useful in a Delta Merge operation where the number of shuffle files can be a bottleneck. Running
   * the merge condition through this method can give an idea about the amount of memory resources required to run the merge.
   *
   * For example, if the condition is "snapshot.id = update.id and country = 'GBR' and age >= 30 and age <= 40 and firstname like '%Jo%' "
   * and country is the partition column, then the output might look like =>
   * Map(
   * OVERALL RESOLVED CONDITION => [ (country = 'GBR') and (age >= 30) and (age <= 40) and firstname LIKE '%Joh%' ] -> 18,
   * GREATER THAN / LESS THAN PART => [ (age >= 30) and (age <= 40) ] -> 100,
   * EQUALS/EQUALS NULL SAFE PART => [ (country = 'GBR') ] -> 300,
   * LEFT OVER PART => [ firstname LIKE '%Joh%' ] -> 600,
   * UNRESOLVED PART => [ (snapshot.id = update.id) ] -> 800,
   * TOTAL_NUM_FILES_IN_DELTA_TABLE => -> 800,
   * UNRESOLVED_COLUMNS => -> List(snapshot.id, update.id))
   *
   * 18 - number of files that will be pulled into memory for the entire provided condition
   * 100 - number of files signifying the greater than/less than part => "age >= 30 and age <= 40"
   * 300 - number of files signifying the equals part => "country = 'GBR'
   * 600 - number of files signifying the like (or any other) part => "firstname like '%Jo%' "
   * 800 - number of files signifying any other part. This is mostly a failsafe
   * 1. to capture any other condition that might have been missed
   * 2. If wrong attribute names or conditions are provided like snapshot.id = source.id (usually found in merge conditions)
   * 800 - Total no. of files in the Delta Table without any filter condition or partitions
   * List() - List of unresolved columns/attributes in the provided condition
   * Note: Whenever a resolved condition comes back as Empty, the output will contain number of files in the entire Delta Table and can be ignored
   * This function works only on the Delta Log and does not scan any data in the Delta Table.
   *
   * @param path
   * @param condition
   * @return
   */
  def getNumShuffleFiles(path: String, condition: String) = {
    val (deltaLog, unresolvedColumns, targetOnlyPredicates, minMaxOnlyExpressions, equalOnlyExpressions,
    otherExpressions, removedPredicates) = getResolvedExpressions(path, condition)
    deltaLog.withNewTransaction { deltaTxn =>
      Map(s"$OVERALL [ ${formatSQL(targetOnlyPredicates).getOrElse("Empty")} ]" ->
        deltaTxn.filterFiles(targetOnlyPredicates).count(a => true),
        s"$MIN_MAX [ ${formatSQL(minMaxOnlyExpressions).getOrElse("Empty")} ]" ->
          deltaTxn.filterFiles(minMaxOnlyExpressions).count(a => true),
        s"$EQUALS [ ${formatSQL(equalOnlyExpressions).getOrElse("Empty")} ]" ->
          deltaTxn.filterFiles(equalOnlyExpressions).count(a => true),
        s"$LEFT_OVER [ ${formatSQL(otherExpressions).getOrElse("Empty")} ]" ->
          deltaTxn.filterFiles(otherExpressions).count(a => true),
        s"$UNRESOLVED [ ${formatSQL(removedPredicates).getOrElse("Empty")} ]" ->
          deltaTxn.filterFiles(removedPredicates).count(a => true),
        TOTAL_NUM_FILES -> deltaLog.snapshot.filesWithStatsForScan(Nil).count(),
        UNRESOLVED_COLS -> unresolvedColumns)
    }
  }

  def getShuffleFileMetadata(path: String, condition: String):
  (Seq[AddFile], Seq[AddFile], Seq[AddFile], Seq[AddFile], Seq[AddFile], DataFrame, Seq[String]) = {
    val (deltaLog, unresolvedColumns, targetOnlyPredicates, minMaxOnlyExpressions, equalOnlyExpressions, otherExpressions, removedPredicates) = getResolvedExpressions(path, condition)
    deltaLog.withNewTransaction { deltaTxn =>
      (deltaTxn.filterFiles(targetOnlyPredicates),
        deltaTxn.filterFiles(minMaxOnlyExpressions),
        deltaTxn.filterFiles(equalOnlyExpressions),
        deltaTxn.filterFiles(otherExpressions),
        deltaTxn.filterFiles(removedPredicates),
        deltaLog.snapshot.filesWithStatsForScan(Nil),
        unresolvedColumns)
    }
  }
  private def getResolvedExpressions(path: String, condition: String) = {
    val spark = SparkSession.active
    val deltaTable = DeltaTable.forPath(path)
    val deltaLog = DeltaLog.forTable(spark, path)

    val expression = functions.expr(condition).expr
    val targetPlan = deltaTable.toDF.queryExecution.analyzed
    val resolvedExpression: Expression = spark.sessionState.analyzer.resolveExpressionByPlanOutput(expression, targetPlan, true)
    val unresolvedColumns = if (!resolvedExpression.childrenResolved) {
      resolvedExpression.references.filter(a => a match {
        case b: UnresolvedAttribute => true
        case _ => false
      }).map(a => a.asInstanceOf[UnresolvedAttribute].sql).toSeq
    } else Seq()

    def splitConjunctivePredicates(condition: Expression): Seq[Expression] = {
      condition match {
        case And(cond1, cond2) =>
          splitConjunctivePredicates(cond1) ++ splitConjunctivePredicates(cond2)
        case other => other :: Nil
      }
    }

    val splitExpressions = splitConjunctivePredicates(resolvedExpression)
    val targetOnlyPredicates = splitExpressions.filter(_.references.subsetOf(targetPlan.outputSet))

    val minMaxOnlyExpressions = targetOnlyPredicates.filter(e => e match {
      case GreaterThanOrEqual(_, _) => true
      case LessThanOrEqual(_, _) => true
      case LessThan(_, _) => true
      case GreaterThan(_, _) => true
      case _ => false
    })

    val equalOnlyExpressions = targetOnlyPredicates.filter(e => e match {
      case EqualTo(_, _) => true
      case EqualNullSafe(_, _) => true
      case _ => false
    })

    val otherExpressions = targetOnlyPredicates.filter(e => e match {
      case EqualTo(_, _) => false
      case EqualNullSafe(_, _) => false
      case GreaterThanOrEqual(_, _) => false
      case LessThanOrEqual(_, _) => false
      case LessThan(_, _) => false
      case GreaterThan(_, _) => false
      case _ => true
    })

    val removedPredicates = splitExpressions.filterNot(_.references.subsetOf(targetPlan.outputSet))

    (deltaLog, unresolvedColumns, targetOnlyPredicates, minMaxOnlyExpressions, equalOnlyExpressions, otherExpressions, removedPredicates)
  }


  private def getAllPartitionStats(filteredDF: DataFrame, groupByCol: String, aggCol: String) = filteredDF
    .groupBy(map_entries(col(groupByCol)))
    .agg(
      count(aggCol),
      mean(aggCol),
      stddev(aggCol),
      min(aggCol),
      max(aggCol),
      percentile_approx(
        col(aggCol),
        lit(Array(0.1, 0.25, 0.50, 0.75, 0.90, 0.95)),
        lit(Int.MaxValue)
      )
    )

  private def deltaFileStats(path: String, condition: Option[String] = None): DataFrame = {
    val tableLog = DeltaLog.forTable(SparkSession.active, path)
    val snapshot = tableLog.snapshot
    condition match {
      case None => snapshot.filesWithStatsForScan(Nil)
      case Some(value) => snapshot.filesWithStatsForScan(Seq(functions.expr(value).expr))
    }
  }

  def deltaFileSizes(deltaTable: DeltaTable) = {
    val details: Row = deltaTable.detail().select("numFiles", "sizeInBytes").collect()(0)
    val (sizeInBytes, numberOfFiles) =
      (details.getAs[Long]("sizeInBytes"), details.getAs[Long]("numFiles"))
    val avgFileSizeInBytes = if (numberOfFiles == 0) 0 else Math.round(sizeInBytes / numberOfFiles)
    Map(
      "size_in_bytes"              -> sizeInBytes,
      "number_of_files"            -> numberOfFiles,
      "average_file_size_in_bytes" -> avgFileSizeInBytes
    )
  }

  /**
   * This function remove all duplicate records from a delta table. Duplicate records means all rows
   * that have more than one occurrence of the value of the columns provided in the input parameter
   * duplicationColumns.
   *
   * @param deltaTable
   *   : delta table object
   * @param duplicateColumns
   *   : collection of columns names that represent the duplication key.
   */
  def killDuplicateRecords(deltaTable: DeltaTable, duplicateColumns: Seq[String]): Unit = {
    val df = deltaTable.toDF

    // 1 Validate duplicateColumns is not empty
    if (duplicateColumns.isEmpty)
      throw new NoSuchElementException("the input parameter duplicateColumns must not be empty")

    // 2 Validate duplicateColumns exists in the delta table.
    JodieValidator.validateColumnsExistsInDataFrame(duplicateColumns, df)

    // 3 execute query statement with windows function that will help you identify duplicated records.
    val duplicatedRecords = df
      .withColumn("quantity", count("*").over(partitionBy(duplicateColumns.map(c => col(c)): _*)))
      .filter("quantity>1")
      .drop("quantity")
      .distinct()

    // 4 execute delete statement to remove duplicate records from the delta table.
    val deleteCondition = duplicateColumns.map(dc => s"old.$dc = new.$dc").mkString(" AND ")
    deltaTable
      .alias("old")
      .merge(duplicatedRecords.alias("new"), deleteCondition)
      .whenMatched()
      .delete()
      .execute()
  }

  /**
   * This function remove duplicate records from a delta table keeping only one occurrence of the
   * deleted record. If not duplicate columns are provided them the primary key is used as partition
   * key to identify duplication.
   *
   * @param deltaTable
   *   : delta table object
   * @param duplicateColumns
   *   : collection of columns names that represent the duplication key.
   * @param primaryKey
   *   : name of the primary key column associated to the delta table.
   */
  def removeDuplicateRecords(
      deltaTable: DeltaTable,
      primaryKey: String,
      duplicateColumns: Seq[String]
  ): Unit = {
    val df = deltaTable.toDF
    // 1 Validate primaryKey is not empty
    if (primaryKey.isEmpty)
      throw new NoSuchElementException("the input parameter primaryKey must not be empty")

    // Validate duplicateColumns is not empty
    if (duplicateColumns.isEmpty)
      throw new NoSuchElementException("the input parameter duplicateColumns must not be empty")

    // 2 Validate if duplicateColumns is not empty that all its columns are in the delta table
    JodieValidator.validateColumnsExistsInDataFrame(duplicateColumns, df)

    // 3 execute query using window function to find duplicate records. Create a match expression to evaluate
    // the case when duplicateColumns is empty and when it is not empty
    val duplicateRecords = df
      .withColumn(
        "row_number",
        row_number().over(partitionBy(duplicateColumns.map(c => col(c)): _*).orderBy(primaryKey))
      )
      .filter("row_number>1")
      .drop("row_number")
      .distinct()

    // 4 execute delete statement  in the delta table
    val deleteCondition =
      (Seq(primaryKey) ++ duplicateColumns).map(c => s"old.$c = new.$c").mkString(" AND ")
    deltaTable
      .alias("old")
      .merge(duplicateRecords.as("new"), deleteCondition)
      .whenMatched()
      .delete()
      .execute()
  }

  def removeDuplicateRecords(deltaTable: DeltaTable, duplicateColumns: Seq[String]): Unit = {
    val df = deltaTable.toDF

    // 1 Validate duplicateColumns is not empty
    if (duplicateColumns.isEmpty)
      throw new NoSuchElementException("the input parameter duplicateColumns must not be empty")

    // 2 Validate duplicateColumns exists in the delta table.
    JodieValidator.validateColumnsExistsInDataFrame(duplicateColumns, df)

    val storagePath = getStorageLocation(deltaTable)

    // 3 execute query statement with windows function that will help you identify duplicated records.
    df
      .dropDuplicates(duplicateColumns)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(storagePath)
  }

  /**
   * Validates and appends data to a Delta table. This function ensures that the provided DataFrame can be appended
   * to the specified Delta table by checking data type compatibility and column presence.
   *
   * @param deltaTable   The Delta table to which data will be appended.
   * @param appendDF     The DataFrame containing data to be appended.
   * @param requiredCols The list of required columns in the appendDF.
   * @param optionalCols The list of optional columns in the appendDF.
   * @throws IllegalArgumentException if input arguments have an invalid type, are missing, or are empty.
   * @throws IllegalArgumentException if required columns are missing in the provided Delta table.
   * @throws IllegalArgumentException if a column in the append DataFrame is not part of the original Delta table.
   */
  def validateAppend(
                      deltaTable: DeltaTable,
                      appendDF: DataFrame,
                      requiredCols: List[String],
                      optionalCols: List[String]
                    ): Unit = {

    val appendDataColumns = appendDF.columns
    val tableColumns = deltaTable.toDF.columns

    // Check if all required columns are present in appendDF
    val missingColumns = requiredCols.filterNot(appendDataColumns.contains)
    require(missingColumns.isEmpty, s"The base Delta table has these columns ${appendDataColumns.mkString("List(", ", ", ")")}, but these columns are required $requiredCols")

    // Check if all columns in appendDF are part of the current Delta table or optional
    val invalidColumns = appendDataColumns.filterNot(column => tableColumns.contains(column) || optionalCols.contains(column))
    require(invalidColumns.isEmpty, s"The following columns are not part of the current Delta table. If you want to add these columns to the table, you must set the optionalCols parameter: ${invalidColumns.mkString("List(", ", ", ")")}")

    val details = deltaTable.detail().select("location").collect().head.getString(0)

    // Write the appendDF to the Delta table
    appendDF.write.format("delta")
      .mode(SaveMode.Append)
      .option("mergeSchema", "true")
      .save(details)
  }

  def getStorageLocation(deltaTable: DeltaTable): String = {
    val row          = deltaTable.detail().select("location").collect().head
    val locationPath = row.getString(0)
    locationPath
  }

  /**
   * This function takes an existing delta table and make an copy of all its data, properties and
   * partitions to a new delta table. The new table could be created based on a specified path or
   * just a given table name, one must take in account that if the table is created using name the
   * path of the table will be the one specified in the spark config property
   * spark.sql.warehouse.dir .
   *
   * @param deltaTable
   *   : delta table object.
   * @param targetPath
   *   : path to directory where the table will be created, this is an optional attribute that can
   *   be replaced by targetTableName.
   * @param targetTableName
   *   : name of the table that will be created.
   */
  def copyTable(
      deltaTable: DeltaTable,
      targetPath: Option[String] = None,
      targetTableName: Option[String] = None
  ): Unit = {
    val details = deltaTable.detail().select("partitionColumns", "properties").collect().head

    val insertStatement = deltaTable.toDF.write
      .format("delta")
      .partitionBy(details.getAs[mutable.ArraySeq[String]]("partitionColumns").toSeq: _*)
      .options(details.getAs[Map[String, String]]("properties"))

    (targetTableName, targetPath) match {
      case (Some(tableName), None) => insertStatement.saveAsTable(tableName)
      case (None, Some(path))      => insertStatement.save(path)
      case (Some(_), Some(_)) =>
        throw JodieValidationError(
          "Ambiguous destination only one of the two must be defined targetPath or targetTableName."
        )
      case (None, None) =>
        throw JodieValidationError("Either targetPath or targetTableName must be specified.")
    }
  }

  /**
   * * This function inserts data into an existing delta table and prevents data duplication in the
   * process.
   *
   * @param deltaTable
   *   : delta table object
   * @param appendData
   *   : new data to be inserted in the existing delta table
   * @param compositeKey
   *   : set of columns that grouped form a unique key inside the table.
   */
  def appendWithoutDuplicates(
      deltaTable: DeltaTable,
      appendData: DataFrame,
      compositeKey: Seq[String]
  ): Unit = {
    if (compositeKey.isEmpty)
      throw new NoSuchElementException("The attribute compositeKey must not be empty")

    val mergeCondition    = compositeKey.map(c => s"old.$c = new.$c").mkString(" AND ")
    val appendDataCleaned = appendData.dropDuplicates(compositeKey)
    deltaTable
      .alias("old")
      .merge(appendDataCleaned.alias("new"), mergeCondition)
      .whenNotMatched()
      .insertAll()
      .execute()
  }

  def findCompositeKeyCandidate(
      deltaTable: DeltaTable,
      excludeCols: Seq[String] = Nil
  ): Seq[String] = {
    val df = deltaTable.toDF

    val cols      = df.columns.toSeq
    val totalCols = cols.length
    val totalRows = df.distinct().count()
    val dfCleaned = df.drop(excludeCols: _*)

    val compositeColumns = for {
      i <- 1 to totalCols + 1
      r <- dfCleaned.columns.combinations(i)
      if dfCleaned.select(r.map(c => col(c)): _*).distinct().count() == totalRows
      if r.length != totalCols
    } yield r

    if (compositeColumns.nonEmpty)
      compositeColumns.head
    else
      Nil
  }

  def withMD5Columns(
      dataFrame: DataFrame,
      cols: List[String],
      newColName: String = ""
  ): DataFrame = {
    val outputCol = if (newColName.isEmpty) cols.mkString("_md5", "", "") else newColName
    dataFrame.withColumn(outputCol, md5(concat_ws("||", cols.map(c => col(c)): _*)))
  }

  def withMD5Columns(
      deltaTable: DeltaTable,
      cols: List[String],
      newColName: String
  ): DataFrame = withMD5Columns(deltaTable.toDF, cols, newColName)

  /**
   * This function validates whether a set of columns is a candidate to be a composite key.
   *
   * @param deltaTable
   *     The Delta table object
   * @param cols
   *     List of columns to be validated
   * @return
   *     True if the columns are unique and eligible to be a composite key, otherwise false.
   */

  def isCompositeKeyCandidate(deltaTable: DeltaTable, cols: List[String]): Boolean = {
    val df: Dataset[Row] = deltaTable.toDF

    val partitionColumn: Seq[Column] = cols.map(col)

    // Candidate columns should not be an empty list
    if (cols.isEmpty)
      throw new NoSuchElementException("At least one column must be specified.")

    // Candidate columns should be part of the DeltaTable
    val areValidColumns: Boolean = cols.forall(col => df.columns.toSeq.contains(col))

    if (!areValidColumns)
      throw new NoSuchElementException(s"The base table has these columns ${df.columns.mkString(",")}, but these columns are required ${cols.mkString(",")}")

    // Apply a function to check for duplicate records based on the specified keys.
    val duplicateRecords =
      deltaTable.toDF
      .groupBy(partitionColumn: _*).count()
      .filter(col("count") > 1)
      .drop(col("count"))

    duplicateRecords.isEmpty
  }

}
