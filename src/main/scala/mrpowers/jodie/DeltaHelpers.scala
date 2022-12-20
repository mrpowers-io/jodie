package mrpowers.jodie

import org.apache.spark.sql.SparkSession
import io.delta.tables._
import org.apache.spark.sql.expressions.Window.partitionBy
import org.apache.spark.sql.functions.{col, count}

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
   * This function remove all duplicate records from a delta table.
   * Duplicate records means all rows that have more than one occurrence
   * of the value of the columns provided in the input parameter duplicationColumns.
   *
   * @param deltaTable: delta table object
   * @param duplicateColumns: collection of columns names that represent the duplication key.
   */
  def removeDuplicateRecords(deltaTable: DeltaTable, duplicateColumns: Seq[String] ): Unit ={
    val df = deltaTable.toDF

    //1 Validate duplicateColumns is not empty
    if(duplicateColumns.isEmpty)
      throw new NoSuchElementException("the input parameter duplicateColumns could not be empty")

    //2 Validate duplicateColumns exists in the delta table.
    val tableColumns = df.columns.toSeq
    if(duplicateColumns.diff(tableColumns).nonEmpty){
      throw JodieValidationError(s"there are columns in duplicateColumns:$duplicateColumns that do not exists in the deltaTable: $tableColumns")
    }

    //3 execute query statement with windows function that will help you identify duplicated records.
    val duplicatedRecords = df
      .withColumn("quantity",count("*").over(partitionBy(duplicateColumns.map(c => col(c)):_*)))
      .filter("quantity>1")
      .drop("quantity")
      .distinct()

    //4 execute delete statement to remove duplicate records from the delta table.
    val deleteCondition = duplicateColumns.map(dc=> s"old.$dc = new.$dc").mkString(" AND ")
    deltaTable.alias("old")
      .merge(duplicatedRecords.alias("new"),deleteCondition)
      .whenMatched()
      .delete()
      .execute()
  }

}
