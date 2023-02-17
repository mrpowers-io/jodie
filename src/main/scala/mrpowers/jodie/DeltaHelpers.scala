package mrpowers.jodie

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.{col, count, row_number}

object DeltaHelpers {

  /**
   * Gets the latest version of a Delta lake
   */
  def latestVersion(path: String): Long = {
    DeltaTable
      .forPath(SparkSession.active, path)
      .history(1)
      .select("version")
      .head()(0)
      .asInstanceOf[Long]
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
      .partitionBy(details.getAs[Seq[String]]("partitionColumns"): _*)
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
   * @param primaryKeysColumns
   *   : set of columns that grouped form a unique key inside the table.
   */
  def appendWithoutDuplicates(
      deltaTable: DeltaTable,
      appendData: DataFrame,
      primaryKeysColumns: Seq[String]
  ): Unit = {
    if (primaryKeysColumns.isEmpty)
      throw new NoSuchElementException("The attribute primaryKeysColumns must not be empty")

    val mergeCondition = primaryKeysColumns.map(c => s"old.$c = new.$c").mkString(" AND ")
    val appendDataCleaned = appendData.dropDuplicates(primaryKeysColumns)
    deltaTable
      .alias("old")
      .merge(appendDataCleaned.alias("new"), mergeCondition)
      .whenNotMatched()
      .insertAll()
      .execute()
  }

}
