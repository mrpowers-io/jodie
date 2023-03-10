package mrpowers.jodie

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.delta.actions.{AddCDCFile, Metadata}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.cdc.CDCReader.isCDCEnabledOnTable
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.{DeltaLog, VersionNotFoundException}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.IOException
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object ChangeDataFeedHelper {
  def apply(path: String, startingVersion: Long): ChangeDataFeedHelper = {
    val deltaLog = DeltaLog.forTable(SparkSession.active, path)
    new ChangeDataFeedHelper(path, startingVersion, deltaLog.snapshot.version, deltaLog)
  }

  def apply(path: String, startingVersion: Long, endingVersion: Long): ChangeDataFeedHelper = {
    val deltaLog = DeltaLog.forTable(SparkSession.active, path)
    new ChangeDataFeedHelper(path, startingVersion, endingVersion, deltaLog)
  }
}

/** *
 * Helper Class and methods for working with different failure scenarios while using Change Data Feed provided by OSS Delta Lake
 * Detailed explanation under : [[https://medium.com/@joydeep.roy/change-data-feed-failure-scenarios-recovery-explained-5606c65d0c2e]]
 *
 * @param path
 * @param startingVersion
 * @param endingVersion
 * @param deltaLog
 */
case class ChangeDataFeedHelper(path: String, startingVersion: Long, endingVersion: Long, deltaLog: DeltaLog) {
  val spark = SparkSession.active

  /** *
   * The quintessential time travel query based on class [[ChangeDataFeedHelper]] provided [starting,ending] versions
   *
   * @return Spark Dataframe with _commit_version, _commit_timestamp and _change_type column which mark the CDC info
   */
  def readCDF: DataFrame = readCDF(this.path, this.startingVersion, this.endingVersion)

  /** *
   * Finds the ranges of versions between {[[startingVersion]] and [[endingVersion]]} for which CDF is available and
   * returns a unionised Dataframe based on these versions skipping the invalid ones.
   */
  def readCDFIgnoreMissingRangesForEDR = getRangesForCDFEnabledVersions.get
    .map(x => readCDF(path, x._1, x._2)).reduce(_ union _)

  /** *
   * Finds the valid versions between which CDF is available and runs time travel query on top of it. [[startingVersion]]
   * will generally be affected if Delta Log is deleted or CDF is disabled for the same
   *
   * @return Spark Dataframe between versions returned by [[getVersionsForAvailableDeltaLog]]
   */
  def readCDFIgnoreMissingDeltaLog = getVersionsForAvailableDeltaLog.map(x => readCDF(path, x._1, x._2)).get

  /** *
   * Finds and loads the valid versions for which underlying change data is available and not vacuumed. [[startingVersion]]
   * will generally be affected if CDC under _change_data directory is deleted for the same
   *
   * @return Spark Dataframe between versions returned by [[getVersionsForAvailableCDC]]
   */
  def readCDFIgnoreMissingCDC = getVersionsForAvailableCDC.map(x => readCDF(path, x._1, x._2)).get

  /** *
   * Can be used to verify that none of the issues expressed in [[https://medium.com/@joydeep.roy/change-data-feed-failure-scenarios-prevention-explained-5606c65d0c2e]]
   * exists. Dry Run will return the same version as passed in the [[ChangeDataFeedHelper]] class. For any other case it would thrown an error or exception:
   * [[AssertionError]] When any of the mentioned issues exist with proper error message to indicate what went wrong
   * [[IllegalStateException]] When any of the methods return None which indicates some deeper issue. It is advisable to run individual methods for debugging
   *
   * @return [[ChangeDataFeedHelper]] should ideally match the [[ChangeDataFeedHelper]] on which it is invoked
   */
  def dryRun(): ChangeDataFeedHelper = (getVersionsForAvailableDeltaLog, getVersionsForAvailableCDC, getRangesForCDFEnabledVersions) match {
    case (Some(a), Some(b), Some(c)) => assert(a == (startingVersion, endingVersion),
      s"Delta Log for provided versions are not available. Available versions are between ${a._1} and ${a._2}")
      assert(b == (startingVersion, endingVersion), s"Change Data for provided versions are not available. Available CDC versions are between ${a._1} and ${a._2}")
      assert(c.size == 1 && c.head == (startingVersion, endingVersion),
        s"CDC has been disabled  between provided versions : $startingVersion and $endingVersion . " +
          s"Use getRangesForCDFEnabled method to find exact versions between which CDC is available")
      this
    case (_, _, _) => throw new IllegalStateException("Please run methods individually to debug issues with CDF.")
  }

  /** *
   * Finds the earliest version for which Delta Log aka Transaction Log aka Version JSON is available
   *
   * @return {[[startingVersion]],[[endingVersion]]} versions with emphasis on [[startingVersion]] which indicates the
   *         earliest version
   */
  def checkEarliestDeltaFileBetweenVersions: Option[(Long, Long)] = getLogVersions(false)

  /** *
   * Checks if time travel is possible between versions and if CDF is enabled for them
   *
   * @return {[[startingVersion]],[[endingVersion]]} versions for which Time Travel is actually possible, since the backing
   *         checkpoint file must be present for the [[startingVersion]]
   */
  def getVersionsForAvailableDeltaLog: Option[(Long, Long)] = getLogVersions(true)

  /**
   * Finds all versions for which CDF is enabled
   */
  def getAllCDFEnabledVersions: List[Long] = getAllVersionsWithCDFStatus.filter(_._2 == true).map(x => x._1)

  /**
   * Finds all versions for which CDF is disabled
   */
  def getAllCDFDisabledVersions: List[Long] = getAllVersionsWithCDFStatus.filter(_._2 == false).map(x => x._1)

  /** *
   * Gets a list of all versions and their corresponding CDF enabled or disabled status
   */
  def getAllVersionsWithCDFStatus: List[(Long, Boolean)] = getCDFVersions(DeltaLog.forTable(spark, path), startingVersion, endingVersion)

  /** *
   * Gets all ranges of versions between which the time travel query would work
   *
   * @return Example : List((0,4),(7,9),(12,15)) denotes version ranges between which CDF is enabled
   */
  def getRangesForCDFEnabledVersions: Option[List[(Long, Long)]] = groupVersionsInclusive(getAllCDFEnabledVersions)

  /** *
   * Gets all ranges of versions between which the time travel query would fail
   *
   * @return Example : List((5,6),(10,11),(16,20)) denotes version ranges between which CDF is disabled
   */
  def getRangesForCDFDisabledVersions: Option[List[(Long, Long)]] = groupVersionsInclusive(getAllCDFDisabledVersions)

  /** *
   * Gets the last available version for which time travel may be possible. Moderated by the [[isCheckpoint]] variable
   * which tells if [[startingVersion]] is the earliest Delta Log file or earliest Checkpoint file
   * The obvious marker exception to call this method is [[VersionNotFoundException]], this is thrown when you run
   * the time travel query
   *
   * @param isCheckpoint
   * @return
   */
  def getLogVersions(isCheckpoint: Boolean): Option[(Long, Long)] = try {
    val history = deltaLog.history
    history.checkVersionExists(startingVersion, isCheckpoint)
    val snapshot = deltaLog.getSnapshotAt(startingVersion)
    if (CDCReader.isCDCEnabledOnTable(snapshot.metadata))
      Some(startingVersion, endingVersion)
    else {
      None
    }
  } catch {
    case versionNotFound: VersionNotFoundException =>
      Some(versionNotFound.earliest, versionNotFound.latest)
  }

  /** *
   * Gets the versions for which CDC data is available. Actually checks the presence of files in delta directory
   * Operations may be time-consuming and memory intensive based on Driver if lot of versions need to be verified.
   * Relies on the vacuum operation completion i.e. if a vacuum operation completed successfully, then checking just one
   * file per version from the _change_data folder should be sufficient. Quits the operation as soon as first file is found
   * in the _change_data directory as it further CDC for upcoming versions would be available generally, if not deleted
   * manually. The obvious marker exception to call this method is [[java.io.FileNotFoundException]], this is thrown
   * when you run the time travel query and underlying data is deleted.
   *
   * @return Example : Some(3,6) - versions for which CDC is present in _change_data folder
   */
  def getVersionsForAvailableCDC = {
    var versionToQuery = -1l
    // Handle Version 0 as it does not have a cdc column
    val start = if (startingVersion == 0l && startingVersion + 1 < endingVersion)
      startingVersion + 1l
    else
      startingVersion
    breakable {
      for (i <- start until endingVersion) {
        val df = spark.read.json(FileNames.deltaFile(deltaLog.logPath, i).toString)
        df.columns.contains("cdc") match {
          case false => //Check if operation is a NoOp MERGE - one which does not update, insert ot delete any rows
            val operationMetrics = df.filter("commitInfo is not null")
              .select("commitInfo.operationMetrics").take(1)(0)
            val metrics = operationMetrics.get(0).asInstanceOf[GenericRowWithSchema]
            assert(metrics.getAs[String]("numTargetRowsInserted") == "0"
              && metrics.getAs[String]("numTargetRowsUpdated") == "0" && metrics.getAs[String]("numTargetRowsDeleted") == "0"
              , "No insert/update/delete happened and cdc column is not present, CDF might have been disabled between versions")
            Unit
          case true =>
            val row = df.select("cdc.path").filter("cdc is not null").take(1)(0)
            val cdfPath = row.get(0).toString
            val fullCDFPath = new Path(deltaLog.dataPath + "/" + cdfPath)
            try {
              // We just check for the first CDF path that is available per version. This is sufficient because a delta table which is
              // not tampered manually, will wither have all CDC files corresponding to a version or have them all vacuumed
              if (fullCDFPath.getFileSystem(new Configuration).getFileStatus(fullCDFPath).isFile())
                versionToQuery = i
              break
            } catch {
              case io: IOException => Unit
            }
        }
      }
    }
    if (versionToQuery == -1l)
      None
    else
      Some(versionToQuery, endingVersion)
  }

  /** *
   * Groups versions based on consecutive integers, assuming missing versions are range terminators.
   *
   * @param versions
   * @return Example : List((0,4),(7,9),(12,45)) denotes version ranges between which CDF is enabled (or disabled)
   */
  def groupVersionsInclusive(versions: List[Long]) = versions.size match {
    case 0 => None
    case _ =>
      var pVersion = versions.head
      var sVersion = versions.head
      var curVersion = -1l
      val ranges: mutable.TreeMap[Long, Long] = mutable.TreeMap.empty
      versions.tail.foreach { x =>
        if (pVersion + 1 == x) {
          curVersion = x
          pVersion = x
        } else {
          ranges.put(sVersion, curVersion)
          pVersion = x
          sVersion = x
        }
      }
      ranges.put(sVersion, curVersion)
      Some(ranges.toList)
  }

  /** *
   * Gets a list of all versions and their corresponding CDF statuses
   * The obvious marker exception to call this method is [[org.apache.spark.sql.delta.DeltaAnalysisException]],
   * this is thrown when you run the time travel query and CDF Enable-Disable-Re-enable has happened multiple times
   *
   * @param deltaLog
   * @param startingVersion
   * @param endingVersion
   * @return
   */
  def getCDFVersions(deltaLog: DeltaLog, startingVersion: Long, endingVersion: Long): List[(Long, Boolean)] = {
    val list = deltaLog.getChanges(startingVersion).takeWhile(_._1 <= endingVersion).toList
    var prev = false
    list.map {
      case (v, actions) =>
        val cdcEnabled = actions.exists {
          case m: Metadata => prev = isCDCEnabledOnTable(m)
            prev
          case c: AddCDCFile => true
          // can't simply return a false here, as previous state needs to be continued
          // a no-op merge operation disrupts this state as cdc column(AddCDCFile) is not present
          // So if we return false here, then an enabled CDF might look like it is disabled for a version
          case _ => prev
        }
        (v, cdcEnabled)
    }
  }

  /** *
   * The quintessential time travel query based on [starting,ending] versions
   *
   * @param path
   * @param startingVersion
   * @param endingVersion
   * @return Spark Dataframe with _commit_version, _commit_timestamp and _change_type column which mark the CDC info
   */
  def readCDF(path: String, startingVersion: Long, endingVersion: Long): DataFrame = spark.read.format("delta")
    .option("readChangeFeed", "true").option("startingVersion", startingVersion).option("endingVersion", endingVersion)
    .load(path)
}
