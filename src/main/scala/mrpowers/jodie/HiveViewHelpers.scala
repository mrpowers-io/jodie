package mrpowers.jodie

import org.apache.spark.sql.{AnalysisException, SparkSession}

object HiveViewHelpers {

  def createOrReplaceHiveView(viewName: String, deltaPath: String, deltaVersion: Long): Unit = {
    val query = s"""
      CREATE OR REPLACE VIEW $viewName
      AS SELECT * FROM delta.`$deltaPath@v$deltaVersion`
    """.stripMargin
    SparkSession.active.sql(query)
  }

  def getTableType(tableName: String): HiveTableType = {
    val query =
      s"""
         | DESCRIBE FORMATTED $tableName
         |""".stripMargin
    try {
      val df = SparkSession.active.sql(query).select("data_type").filter("col_name = 'Type'")
      df.collect().head.getString(0) match {
        case HiveTableType.MANAGED.label => HiveTableType.MANAGED
        case HiveTableType.EXTERNAL.label => HiveTableType.EXTERNAL
      }
    } catch {
      case e: AnalysisException
        if e.getMessage().toLowerCase().contains("table or view not found") => HiveTableType.NONREGISTERED
    }
  }

  sealed abstract class HiveTableType(val label: String)

  object HiveTableType {
    final case object MANAGED extends HiveTableType(label = "MANAGED")

    final case object EXTERNAL extends HiveTableType(label = "EXTERNAL")

    final case object NONREGISTERED extends HiveTableType(label = "NONREGISTERED")
  }
}
