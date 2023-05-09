package mrpowers.jodie.delta

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
}
