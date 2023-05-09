package mrpowers.jodie

import io.delta.tables._
import mrpowers.jodie.delta.DeltaConstants._
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable

object DeltaHelpers {

  /**
   * Gets the latest version of a Delta lake
   */
  def latestVersion(path: String): Long =
    DeltaLog.forTable(SparkSession.active, path).snapshot.version

  def deltaFileSizesDistribution(path: String, condition: Option[String] = None): DataFrame =
    deltaFileStats(path, condition).select(sizeColumn).summary().toDF(sizeDFColumns: _*)

  def deltaNumRecordsDistribution(path: String, condition: Option[String] = None): DataFrame =
    deltaFileStats(path, condition).select(numRecordsColumn).summary().toDF(numRecordsDFColumns: _*)

  def deltaPartitionWiseFileSizeDistribution(path: String): DataFrame =
    getAllPartitionStats(path, statsPartitionColumn, sizeColumn)
      .toDF(sizeDFColumns: _*)

  def deltaPartitionWiseNumRecordsDistribution(path: String): DataFrame =
    getAllPartitionStats(path, statsPartitionColumn, numRecordsColumn)
      .toDF(numRecordsDFColumns: _*)

  def getAllPartitionStats(path: String, groupByCol: String, aggCol: String) = {
    deltaFileStats(path)
      .withColumn("norm",when(col(aggCol).equalTo(sizeColumn), col(aggCol).divide(1024*1024)).otherwise(col(aggCol)))
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
          lit(2147483647)
        )
      )
  }

  def deltaFileStats(path: String, condition: Option[String] = None): DataFrame = {
    val tableLog = DeltaLog.forTable(SparkSession.active, path)
    val snapshot = tableLog.snapshot
    condition match {
      case None        => snapshot.filesWithStatsForScan(Nil)
      case Some(value) => snapshot.filesWithStatsForScan(Seq(expr(value).expr))
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
    deltaTable.optimize().where("date='2021-11-18'").executeZOrderBy()
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
}
