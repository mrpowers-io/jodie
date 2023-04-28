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

case class UpdateMetric(
    numRemovedFiles: Long,
    numCopiedRows: Long,
    numAddedChangeFiles: Long,
    executionTimeMs: Long,
    scanTimeMs: Long,
    numAddedFiles: Long,
    numUpdatedRows: Long,
    rewriteTimeMs: Long
) extends OperationMetrics
case class WriteMetric(numFiles: Long, numOutputRows: Long, numOutputBytes: Long)
    extends OperationMetrics

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
