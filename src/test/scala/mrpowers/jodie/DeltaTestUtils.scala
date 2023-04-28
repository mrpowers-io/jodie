package mrpowers.jodie

import io.delta.tables.DeltaTable

object DeltaTestUtils extends SparkSessionTestWrapper{
  def executeMergeFor(tableName: String, deltaTable: DeltaTable, updates: List[(Int, String, Int)]) = {
    import spark.implicits._
    updates.foreach(row => {
      val dataFrame = Seq(row).toDF("id", "gender", "age")
      deltaTable.as(tableName)
        .merge(dataFrame.as("source"), s"${tableName}.id = source.id")
        .whenMatched
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    })
    deltaTable
  }

  def executeMergeWithReducedSearchSpace(tableName: String, deltaTable: DeltaTable, updates: List[(Int, String, Int,String)], condition:String)={
    import spark.implicits._
    updates.foreach(row => {
      val dataFrame = Seq(row).toDF("id", "gender", "age","country")
      deltaTable.as(tableName)
        .merge(dataFrame.as("source"), s"${tableName}.id = source.id and $condition")
        .whenMatched
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    })
    deltaTable
  }
}
