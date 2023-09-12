package mrpowers.jodie.delta

import org.apache.spark.sql.catalyst.expressions.Expression

object DeltaConstants {
  val sizeColumn                   = "size"
  val numRecordsColumn             = "stats.numRecords"
  val statsPartitionColumn         = "partitionValues"
  private val percentileCol        = "Percentile[10th, 25th, Median, 75th, 90th, 95th]"
  private val num_of_parquet_files = "num_of_parquet_files"
  val numRecordsDFColumns =
    Array(
      statsPartitionColumn,
      num_of_parquet_files,
      "mean_num_records_in_files",
      "stddev",
      "min_num_records",
      "max_num_records",
      percentileCol
    )

  val sizeDFColumns =
    Array(
      statsPartitionColumn,
      num_of_parquet_files,
      "mean_size_of_files",
      "stddev",
      "min_file_size",
      "max_file_size",
      percentileCol
    )
  val OVERALL = "OVERALL RESOLVED CONDITION =>"
  val MIN_MAX = "GREATER THAN / LESS THAN PART =>"
  val EQUALS = "EQUALS/EQUALS NULL SAFE PART =>"
  val LEFT_OVER = "LEFT OVER PART =>"
  val UNRESOLVED = "UNRESOLVED PART =>"
  val TOTAL_NUM_FILES = "TOTAL_NUM_FILES_IN_DELTA_TABLE =>"
  val UNRESOLVED_COLS = "UNRESOLVED_COLUMNS =>"

  def formatSQL(expressions: Seq[Expression]) = expressions.isEmpty match {
    case true => None
    case false => Some(expressions.map(a => a.sql).reduce(_ + " and " + _))
  }
}
