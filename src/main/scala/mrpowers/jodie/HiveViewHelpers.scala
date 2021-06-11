package mrpowers.jodie

import org.apache.spark.sql.SparkSession

object HiveViewHelpers {

  def createOrReplaceHiveView(viewName: String, deltaPath: String, deltaVersion: Long): Unit = {
    val query = s"""
      CREATE OR REPLACE VIEW $viewName
      AS SELECT * FROM delta.`$deltaPath@v$deltaVersion`
    """.stripMargin
    SparkSession.active.sql(query)
  }

}
