package mrpowers.jodie

import org.apache.spark.sql.{DataFrame, SparkSession}
import io.delta.tables._

object Type2Scd {

  def upsert(path: String, updatesDF: DataFrame, primaryKey: String, attrColNames: Seq[String]): Unit = {
    // @todo error out if the underling Delta DF doesn't follow our conventions
    // @todo error out if updatesDF doesn't follow the right conventions
    val baseTable = DeltaTable.forPath(SparkSession.active, path)
    val updatesAttrs = attrColNames.map(attr => f"updates.$attr <> base.$attr").mkString(" OR ")
    val stagedUpdatesAttrs = attrColNames.map(attr => f"staged_updates.$attr <> base.$attr").mkString(" OR ")
    val stagedPart1 = updatesDF
      .as("updates")
      .join(baseTable.toDF.as("base"), primaryKey)
      .where(f"base.is_current = true AND ($updatesAttrs)")
      .selectExpr("NULL as mergeKey", "updates.*")
    val stagedPart2 = updatesDF.selectExpr(f"$primaryKey as mergeKey", "*")
    val stagedUpdates = stagedPart1.union(stagedPart2)
    baseTable
      .as("base")
      .merge(stagedUpdates.as("staged_updates"), f"base.$primaryKey = mergeKey")
      .whenMatched(f"base.is_current = true AND ($stagedUpdatesAttrs)")
      .updateExpr(Map(
        "is_current" -> "false",
        "end_time" -> "staged_updates.effective_time"))
      .whenNotMatched()
      .insertExpr(Map(primaryKey -> f"staged_updates.$primaryKey") ++
        attrColNames.map(attr => (attr, f"staged_updates.$attr")).toMap ++
        Map("is_current" -> "true",
        "effective_time" -> "staged_updates.effective_time",
        "end_time" -> "null"))
      .execute()
  }

}
