package mrpowers.jodie.delta

sealed trait OperationMetrics

case class DeleteMetric(
                         numDeletedRows: Long,
                         numAddedFiles: Long,
                         numCopiedRows: Long,
                         numRemovedFiles: Long,
                         numAddedChangeFiles: Long,
                         numRemovedBytes: Long,
                         numAddedBytes: Long,
                         executionTimeMs: Long,
                         scanTimeMs: Long,
                         rewriteTimeMs: Long
                       ) extends OperationMetrics
case class RestoreMetric(
                          numRestoredFiles: Long,
                          removedFilesSize: Long,
                          numRemovedFiles: Long,
                          restoredFilesSize: Long,
                          numOfFilesAfterRestore: Long,
                          tableSizeAfterRestore: Long
                        ) extends OperationMetrics

case class TBLProperties(enableCDF: Option[Boolean]) extends OperationMetrics

case class WriteMetric(numFiles: Long, numOutputRows: Long, numOutputBytes: Long)
  extends OperationMetrics

case class VacuumMetric(numFilesToDelete: Long, sizeOfDataToDelete: Long) extends OperationMetrics

case class CompactionMetric(
                             numRemovedFiles: Long,
                             numRemovedBytes: Long,
                             p25FileSize: Long,
                             minFileSize: Long,
                             numAddedFiles: Long,
                             maxFileSize: Long,
                             p75FileSize: Long,
                             p50FileSize: Long,
                             numAddedBytes: Long
                           ) extends OperationMetrics

case class ZOrderByMetric(predicate: Seq[String], columns: Seq[String])

case class MergeMetric(
                        numTargetRowsCopied: Long,
                        numTargetRowsDeleted: Long,
                        numTargetFilesAdded: Long,
                        executionTimeMs: Long,
                        numTargetRowsInserted: Long,
                        scanTimeMs: Long,
                        numTargetRowsUpdated: Long,
                        numOutputRows: Long,
                        numTargetChangeFilesAdded: Long,
                        numSourceRows: Long,
                        numTargetFilesRemoved: Long,
                        rewriteTimeMs: Long
                      ) extends OperationMetrics
