package mrpowers.jodie

import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.delta.DeltaAnalysisException
import org.apache.spark.sql.{AnalysisException, SparkSession}

object HiveHelpers {

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

      val df = SparkSession.active.sql(query).select("data_type","col_name").cache()
      val providerDF = df.filter("lower(col_name) = 'provider'")
      val providerStr = providerDF.collect().head.getString(0)
      providerStr match {
        case "delta" =>
          val typeDF = df.filter("lower(col_name) = 'external' and lower(data_type) = 'true'")
          val isTableTypeExternal = typeDF.collect().nonEmpty
          if (isTableTypeExternal) {
            HiveTableType.EXTERNAL
          } else {
            HiveTableType.MANAGED
          }
        case _ =>
          val typeDF = df.filter("col_name = 'Type'")
          val tableTypeStr = typeDF.collect().head.getString(0)
          tableTypeStr match {
            case HiveTableType.MANAGED.label => HiveTableType.MANAGED
            case HiveTableType.EXTERNAL.label => HiveTableType.EXTERNAL
          }
      }

    } catch {
      case e: AnalysisException
        if e.getMessage().toLowerCase().contains("table or view not found") => HiveTableType.NONREGISTERED
    }
  }

  def registerTable(tableName:String, tablePath:String):Unit = {
    if(tablePath.isEmpty || tableName.isEmpty){
      throw JodieValidationError("tableName and tablePath input parameters must not be empty")
    }
    try{
      SparkSession.active.sql(s"CREATE TABLE $tableName using delta location '$tablePath'")
    }catch {
      case e:DeltaAnalysisException => throw JodieValidationError(s"table:$tableName location:$tablePath is not a delta table")
      case e:TableAlreadyExistsException => throw JodieValidationError(s"table:$tableName already exits")
    }
  }

  sealed abstract class HiveTableType(val label: String)

  object HiveTableType {
    final case object MANAGED extends HiveTableType(label = "MANAGED")

    final case object EXTERNAL extends HiveTableType(label = "EXTERNAL")

    final case object NONREGISTERED extends HiveTableType(label = "NONREGISTERED")
  }
}
