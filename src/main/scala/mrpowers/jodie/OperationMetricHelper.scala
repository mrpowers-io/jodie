package mrpowers.jodie

import mrpowers.jodie.delta._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.{DeltaHistory, DeltaLog}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{LongType, MapType, StringType, StructType}
import org.apache.spark.sql.{Encoders, SparkSession}

case class OperationMetricHelper(
    path: String,
    startingVersion: Long = 0,
    endingVersion: Option[Long] = None
)(implicit spark: SparkSession) {
  private val deltaLog      = DeltaLog.forTable(spark, path)
  private val metricColumns = Seq("version", "deleted", "inserted", "updated", "source_rows")

  /**
   * The function returns operation metrics - count metric for either a provided partition condition
   * or without one, provides the count metric for the entire Delta Table as a Spark Dataframe.
   * +-------+-------+--------+-------+-----------+
   * |version|deleted|inserted|updated|source_rows|
   * +-------+-------+--------+-------+-----------+
   * |6      |0      |108     |0      |108        |
   * |5      |12     |0       |0      |0          |
   * |4      |0      |0       |300    |300        |
   * |3      |0      |100     |0      |100        |
   * |2      |0      |150     |190    |340        |
   * |1      |0      |0       |200    |200        |
   * |0      |0      |400     |0      |400        |
   * +-------+-------+--------+-------+-----------+
   *
   * @param partitionCondition
   * @return
   * [[org.apache.spark.sql.DataFrame]]
   */
  def getCountMetricsAsDF(partitionCondition: Option[String] = None) = {
    import spark.implicits._
    getCountMetrics(partitionCondition).toDF(metricColumns: _*)
  }

  /**
   * The function returns operation metrics - count metric for either a provided partition condition
   * or without one, provides the count metric for the entire Delta Table as a Seq[(Long, Long,
   * Long, Long, Long)].
   *
   * @param partitionCondition
   * @return
   *   [[Seq]] of [[Tuple5]], where element is a [[Long]]
   */
  def getCountMetrics(
      partitionCondition: Option[String] = None
  ): Seq[(Long, Long, Long, Long, Long)] = {
    val histories = partitionCondition match {
      case None => deltaLog.history.getHistory(startingVersion, endingVersion)
      case Some(condition) =>
        deltaLog.history
          .getHistory(startingVersion, endingVersion)
          .filter(x => filterHistoryByPartition(x, condition))
    }
    transformMetric(generateMetric(histories, partitionCondition))
  }

  /**
   * This function returns the records inserted count for WRITE/APPEND operation on a Delta Table
   * partition for a version. It inspects the Delta Log aka Transaction Log to obtain this metric.
   * @param partitionCondition
   * @param version
   * @return
   *   [[Long]]
   */
  def getWriteMetricByPartition(
      partitionCondition: String,
      version: Long
  ): Long = {
    val conditions = splitConditionTo(partitionCondition).map(x => s"${x._1}=${x._2}")
    val jsonSchema = new StructType()
      .add("numRecords", LongType)
      .add("minValues", MapType(StringType, StringType))
      .add("maxValues", MapType(StringType, StringType))
      .add("nullCount", MapType(StringType, StringType))
    spark.read
      .json(FileNames.deltaFile(deltaLog.logPath, version).toString)
      .withColumn("stats", from_json(col("add.stats"), jsonSchema))
      .select("add.path", "stats")
      .map(x => {
        val path = x.getAs[String]("path")
        conditions.map(x => path != null && path.contains(x)).reduceOption(_ && _) match {
          case None => 0L
          case Some(bool) =>
            if (bool)
              x.getAs[String]("stats").asInstanceOf[GenericRowWithSchema].getAs[Long]("numRecords")
            else 0L
        }
      })(Encoders.scalaLong)
      .reduce(_ + _)
  }

  /**
   * Filter and maps the relevant operation for providing count metric: MERGE, WRITE, DELETE and
   * UPDATE
   *
   * @param metric
   * @return
   */
  private def transformMetric(
      metric: Seq[(Long, OperationMetrics)]
  ): Seq[(Long, Long, Long, Long, Long)] = metric.flatMap { case (version, opMetric) =>
    opMetric match {
      case MergeMetric(_, deleted, _, _, inserted, _, updated, _, _, sourceRows, _, _) =>
        Seq((version, deleted, inserted, updated, sourceRows))
      case WriteMetric(_, inserted, _) => Seq((version, 0L, inserted, 0L, inserted))
      case DeleteMetric(deleted, _, _, _, _, _, _, _, _, _) =>
        Seq((version, deleted, 0L, 0L, 0L))
      case UpdateMetric(_, _, _, _, _, _, updated, _) => Seq((version, 0L, 0L, updated, 0L))
      case _                                          => Seq.empty
    }
  }

  /**
   * Given a [[DeltaHistory]] and a partition condition string, this method returns whether the
   * condition matches the partition condition applied to the operations like DELETE, UPDATE and
   * MERGE.
   * @param x
   * @param y
   * @return
   */
  def parseDeltaLogToValidatePartitionCondition(x: DeltaHistory, y: String): Boolean = {
    val inputConditions: Map[String, String] = splitConditionTo(y.toLowerCase)
    val opParamInDeltaLog: Map[String, String] = splitConditionTo(
      // targets a delta log delete string that looks like ["(((country = 'USA') AND (gender = 'Female')) AND (id = 2))"]
      x.operationParameters("predicate")
        .toLowerCase
        .replaceAll("[()]", " ")
        .replaceAll("[\\[\\]]", " ")
        .replaceAll("\\\"", " ")
    )
    inputConditions
      .map(x => if (opParamInDeltaLog.contains(x._1)) opParamInDeltaLog(x._1) == x._2 else false)
      .reduceOption(_ && _) match {
      case None    => false
      case Some(b) => b
    }
  }

  /**
   * Breaks down a string condition into [[Map]] of {[[String]],[[String]]} Handles the
   * idiosyncrasies of Delta Log recorded predicate strings for operations like DELETE, UPDATE and
   * MERGE
   * @param partitionCondition
   * @return
   */
  def splitConditionTo(partitionCondition: String): Map[String, String] = {
    val trimmed = partitionCondition.trim
    val splitCondition =
      if (trimmed.contains(" and "))
        trimmed.split(" and ").toSeq
      else Seq(trimmed)
    splitCondition
      .map(x => {
        val kv = x.split("=")
        assert(kv.size == 2)
        if (kv.head.contains("#")) {
          // targets an update string that looks like (((country#590 = USA) AND (gender#588 = Female)) AND (id#587 = 4))
          kv.head.split("#")(0).trim -> kv.tail.head.trim.stripPrefix("\'").stripSuffix("\'")
        } else if (kv.head.contains("."))
          // targets a merge string that looks like
          // (((multi_partitioned_snapshot.id = source.id) AND (multi_partitioned_snapshot.country = 'IND')) AND
          // (multi_partitioned_snapshot.gender = 'Male'))
          kv.head.split("\\.")(1).trim -> kv.tail.head.trim.stripPrefix("\'").stripSuffix("\'")
        else
          kv.head.trim -> kv.tail.head.trim.stripPrefix("\'").stripSuffix("\'")
      })
      .toMap
  }

  private def filterHistoryByPartition(x: DeltaHistory, partitionCondition: String): Boolean =
    x.operation match {
      case "WRITE" => true
      case "DELETE" | "MERGE" | "UPDATE" =>
        if (
          x.operationParameters
            .contains("predicate") && x.operationParameters.get("predicate") != None
        ) {
          parseDeltaLogToValidatePartitionCondition(x, partitionCondition)
        } else {
          false
        }
      case _ => false
    }

  private def generateMetric(
      deltaHistories: Seq[DeltaHistory],
      partitionCondition: Option[String]
  ): Seq[(Long, OperationMetrics)] =
    deltaHistories
      .map(dh => {
        (
          dh.version.get, {
            val metrics = dh.operationMetrics.get
            dh.operation match {
              case "MERGE" =>
                MergeMetric(
                  numTargetRowsCopied = metrics("numTargetRowsCopied").toLong,
                  numTargetRowsDeleted = metrics("numTargetRowsDeleted").toLong,
                  numTargetFilesAdded = metrics("numTargetFilesAdded").toLong,
                  executionTimeMs = metrics("executionTimeMs").toLong,
                  numTargetRowsInserted = metrics("numTargetRowsInserted").toLong,
                  scanTimeMs = metrics("scanTimeMs").toLong,
                  numTargetRowsUpdated = metrics("numTargetRowsUpdated").toLong,
                  numOutputRows = metrics("numOutputRows").toLong,
                  numTargetChangeFilesAdded = metrics("numTargetChangeFilesAdded").toLong,
                  numSourceRows = metrics("numSourceRows").toLong,
                  numTargetFilesRemoved = metrics("numTargetFilesRemoved").toLong,
                  rewriteTimeMs = metrics("rewriteTimeMs").toLong
                )
              case "WRITE" =>
                partitionCondition match {
                  case None =>
                    WriteMetric(
                      numFiles = metrics("numFiles").toLong,
                      numOutputRows = metrics("numOutputRows").toLong,
                      numOutputBytes = metrics("numOutputBytes").toLong
                    )
                  case Some(condition) =>
                    WriteMetric(0L, getWriteMetricByPartition(condition, dh.version.get), 0L)
                }
              case "DELETE" =>
                DeleteMetric(
                  numDeletedRows = whenContains(metrics, "numDeletedRows"),
                  numAddedFiles = whenContains(metrics, "numAddedFiles"),
                  numCopiedRows = whenContains(metrics, "numCopiedRows"),
                  numRemovedFiles = whenContains(metrics, "numRemovedFiles"),
                  numAddedChangeFiles = whenContains(metrics, "numAddedChangeFiles"),
                  numRemovedBytes = whenContains(metrics, "numRemovedBytes"),
                  numAddedBytes = whenContains(metrics, "numAddedBytes"),
                  executionTimeMs = whenContains(metrics, "executionTimeMs"),
                  scanTimeMs = whenContains(metrics, "scanTimeMs"),
                  rewriteTimeMs = whenContains(metrics, "rewriteTimeMs")
                )
              case "UPDATE" =>
                UpdateMetric(
                  numRemovedFiles = whenContains(metrics, "numRemovedFiles"),
                  numCopiedRows = whenContains(metrics, "numCopiedRows"),
                  numAddedChangeFiles = whenContains(metrics, "numAddedChangeFiles"),
                  executionTimeMs = whenContains(metrics, "executionTimeMs"),
                  scanTimeMs = whenContains(metrics, "scanTimeMs"),
                  numAddedFiles = whenContains(metrics, "numAddedFiles"),
                  numUpdatedRows = whenContains(metrics, "numUpdatedRows"),
                  rewriteTimeMs = whenContains(metrics, "rewriteTimeMs")
                )
              case _ => null
            }
          }
        )
      })
      .filter(x => x._2 != null)

  private def whenContains(map: Map[String, String], key: String) =
    if (map.contains(key)) map(key).toLong else 0L
}
