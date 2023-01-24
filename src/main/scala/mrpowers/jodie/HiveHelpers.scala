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
    try {

      val table = SparkSession.active.catalog.getTable(tableName)

      table.tableType.toUpperCase() match {
        case HiveTableType.MANAGED.label => HiveTableType.MANAGED
        case HiveTableType.EXTERNAL.label => HiveTableType.EXTERNAL
      }
    } catch {
      case e: AnalysisException
        if e.getMessage().toLowerCase().contains(s"table or view '$tableName' not found") => HiveTableType.NONREGISTERED
    }
  }

  def registerTable(tableName:String, tablePath:String, provider:HiveProvider = HiveProvider.DELTA):Unit = {
    if(tablePath.isEmpty || tableName.isEmpty){
      throw JodieValidationError("tableName and tablePath input parameters must not be empty")
    }
    try{
      if(provider == HiveProvider.DELTA){
        SparkSession.active.sql(s"CREATE TABLE $tableName using delta location '$tablePath'")
      }else{
        SparkSession.active.catalog.createTable(tableName,tablePath)
      }

    }catch {
      case e:DeltaAnalysisException => throw JodieValidationError(s"table:$tableName location:$tablePath is not a delta table")
      case e:TableAlreadyExistsException => throw JodieValidationError(s"table:$tableName already exits")
    }
  }

  sealed abstract class HiveTableType(val label: String)

  sealed abstract class HiveProvider(val label: String)

  object HiveProvider{
    final case object DELTA extends HiveProvider(label = "delta")
    final case object PARQUET extends HiveProvider(label = "parquet")
  }

  object HiveTableType {
    final case object MANAGED extends HiveTableType(label = "MANAGED")

    final case object EXTERNAL extends HiveTableType(label = "EXTERNAL")

    final case object NONREGISTERED extends HiveTableType(label = "NONREGISTERED")
  }
}
