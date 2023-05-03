package mrpowers.jodie.delta

object DeltaConstants {
  val sizeColumn           = "size"
  val numRecordsColumn     = "stats.numRecords"
  val statsPartitionColumn = "partitionValues"
  val numRecordsDFColumns =
    Array(
      "num_of_parquet_files",
      "mean_num_records_in_files",
      "stddev",
      "min_num_records",
      "max_num_records"
    )
  val sizeDFColumns =
    Array("num_of_parquet_files", "mean_size_of_files", "stddev", "min_file_size", "max_file_size")
}
