package mrpowers.jodie

import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._

object Type2Scd {

  def upsert(
      table: DeltaTable,
      updatesDF: DataFrame,
      primaryKey: String,
      attrColNames: Seq[String]
  ): Unit = {
    genericUpsert(
      table,
      updatesDF,
      primaryKey,
      attrColNames,
      "is_current",
      "effective_time",
      "end_time"
    )
  }

  def genericUpsert(
      baseTable: DeltaTable,
      updatesDF: DataFrame,
      primaryKey: String,
      attrColNames: Seq[String],
      isCurrentColName: String,
      effectiveTimeColName: String,
      endTimeColName: String
  ): Unit = {
    // validate the existing Delta table
    val baseColNames = baseTable.toDF.columns.toSeq
    val requiredBaseColNames =
      Seq(primaryKey) ++ attrColNames ++ Seq(isCurrentColName, effectiveTimeColName, endTimeColName)
    // @todo move the validation logic to a separate abstraction
    if (baseColNames.sorted != requiredBaseColNames.sorted) {
      throw JodieValidationError(
        f"The base table has these columns '$baseColNames', but these columns are required '$requiredBaseColNames'"
      )
    }
    // validate the updates DataFrame
    val updatesColNames         = updatesDF.columns.toSeq
    val requiredUpdatesColNames = Seq(primaryKey) ++ attrColNames ++ Seq(effectiveTimeColName)
    if (updatesColNames.sorted != requiredUpdatesColNames.sorted) {
      throw JodieValidationError(
        f"The updates DataFrame has these columns '$updatesColNames', but these columns are required '$requiredUpdatesColNames'"
      )
    }
    // perform the upsert
    val updatesAttrs = attrColNames.map(attr => f"updates.$attr <> base.$attr").mkString(" OR ")
    val stagedUpdatesAttrs =
      attrColNames.map(attr => f"staged_updates.$attr <> base.$attr").mkString(" OR ")
    val stagedPart1 = updatesDF
      .as("updates")
      .join(baseTable.toDF.as("base"), primaryKey)
      .where(f"base.$isCurrentColName = true AND ($updatesAttrs)")
      .selectExpr("NULL as mergeKey", "updates.*")
    val stagedPart2   = updatesDF.selectExpr(f"$primaryKey as mergeKey", "*")
    val stagedUpdates = stagedPart1.union(stagedPart2)
    baseTable
      .as("base")
      .merge(stagedUpdates.as("staged_updates"), f"base.$primaryKey = mergeKey")
      .whenMatched(f"base.$isCurrentColName = true AND ($stagedUpdatesAttrs)")
      .updateExpr(
        Map(isCurrentColName -> "false", endTimeColName -> f"staged_updates.$effectiveTimeColName")
      )
      .whenNotMatched()
      .insertExpr(
        Map(primaryKey -> f"staged_updates.$primaryKey") ++
          attrColNames.map(attr => (attr, f"staged_updates.$attr")).toMap ++
          Map(
            isCurrentColName     -> "true",
            effectiveTimeColName -> f"staged_updates.$effectiveTimeColName",
            endTimeColName       -> "null"
          )
      )
      .execute()
  }

}
