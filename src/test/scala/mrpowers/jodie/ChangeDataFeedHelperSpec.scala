package mrpowers.jodie

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.delta.util.FileNames
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, equal}

import java.nio.file.{Files, Paths}

class ChangeDataFeedHelperSpec extends AnyFunSpec
  with SparkSessionTestWrapper
  with DataFrameComparer
  with BeforeAndAfterEach {
  var writePath = ""
  spark.conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

  override def afterEach(): Unit = {
    val tmpDir = os.pwd / "tmp" / "delta-cdf-edr"
    os.remove.all(tmpDir)
  }

  describe("When CDF is enabled on Delta Table") {
    val path = (os.pwd / "tmp" / "delta-cdf-edr").toString()
    val rows = Seq((1, "Male", 25), (2, "Male", 25), (3, "Female", 35))

    val updates = List(
      (1, "Male", 35), (2, "Male", 100), (2, "Male", 101), (2, "Male", 102), (4, "Female", 18),
      (1, "Other", 55), (2, "Male", 65), (2, "Other", 66), (2, "Other", 67), (4, "Female", 25),
      (4, "Other", 45), (2, "Male", 45), (2, "Other", 67), (2, "Other", 345), (4, "Female", 678)
    )

    import spark.implicits._
    val snapshotDF = rows.toDF("id", "gender", "age")

    describe("should return provided versions as valid when") {
      it("Case I - Delta Log is deleted : version is above 10 and checkpoint is present but version 0.json is deleted") {
        setUp("valid_snapshot", snapshotDF, path, updates)
        Files.deleteIfExists(Paths.get(FileNames.deltaFile(new Path(writePath + "/_delta_log"), 0).toString))
        val changeDataFeedHelper = ChangeDataFeedHelper(writePath, 11, 13)
        val actualVersions = changeDataFeedHelper.getVersionsForAvailableDeltaLog
        val expectedVersions = Some(11l, 13l)
        val checkDeltaLogVersion = changeDataFeedHelper.checkEarliestDeltaFileBetweenVersions
        checkDeltaLogVersion.get should equal(11l, 13l)
        actualVersions should equal(expectedVersions)
        val actualDF = changeDataFeedHelper.readCDFIgnoreMissingDeltaLog.get
        actualDF.select("_commit_version").distinct().count() should equal(3l)
        val actualCDCPresentVersions = changeDataFeedHelper.getVersionsForAvailableCDC
        actualCDCPresentVersions should equal(Some(11l, 13l))

      }
      it("All Cases Combined : dryRun API usage to check no CDF read issues exist") {
        setUp("dry_run_snapshot", snapshotDF, path, updates)
        val expected = ChangeDataFeedHelper(writePath, 9, 13)
        val actual = expected.dryRun()
        actual should equal(expected)
        val actualDF = actual.readCDF
        actualDF.select("_commit_version").distinct().count() should equal(5l)
      }
    }
    describe("should not return provided versions and return actual queryable versions that work when") {
      it("Case I - Delta Log is deleted : version is below 10 and 000.json is deleted") {
        val name = "invalid_snapshot"
        setUp(name, snapshotDF, path, updates)
        Files.deleteIfExists(Paths.get(FileNames.deltaFile(new Path(writePath + "/_delta_log"), 0).toString))
        val changeDataFeedHelper = ChangeDataFeedHelper(writePath, 0, 5)
        val validVersions = changeDataFeedHelper.getVersionsForAvailableDeltaLog
        val expectedVersion = Some(10l, 15l)
        val checkDeltaLogVersion = changeDataFeedHelper.checkEarliestDeltaFileBetweenVersions
        checkDeltaLogVersion.get should equal(1l, 15l)
        validVersions should equal(expectedVersion)
        val actualDF = changeDataFeedHelper.readCDFIgnoreMissingDeltaLog.get
        actualDF.select("_commit_version").distinct().count() should equal(6l)
      }
      it("Case II - CDC is deleted : underlying data is deleted from _change_data folder") {
        setUp("cdc_snapshot", snapshotDF, path, updates)
        val firstVersion = FileNames.deltaFile(new Path(writePath + "/_delta_log"), 1).toString
        val row = spark.read.json(firstVersion).select("cdc.path")
          .filter("cdc is not null").take(1)(0)
        val cdfPath = row.get(0).toString
        Files.deleteIfExists(Paths.get(writePath + "/" + cdfPath))
        val changeDataFeedHelper = ChangeDataFeedHelper(writePath, 0, 5)
        val actualVersion = changeDataFeedHelper.getVersionsForAvailableCDC
        val expectedVersion = Some(2l, 5l)
        actualVersion should equal(expectedVersion)
        val actualDF = changeDataFeedHelper.readCDFIgnoreMissingCDC.get
        actualDF.select("_commit_version").distinct().count() should equal(4l)
      }
      it("Case II - CDC is deleted : underlying data is deleted from _change_data folder and has a fake delete resulting in a no op merge") {
        val name = "cdc_delete_snapshot"
        noOpDelete(name, setUp(name, snapshotDF, path, updates), updates.take(4), false)
        val eighteenth = FileNames.deltaFile(new Path(writePath + "/_delta_log"), 15).toString
        val row = spark.read.json(eighteenth).select("cdc.path")
          .filter("cdc is not null").take(1)(0)
        val cdfPath = row.get(0).toString
        Files.deleteIfExists(Paths.get(writePath + "/" + cdfPath))
        val changeDataFeedHelper = ChangeDataFeedHelper(writePath, 15, 18)
        val actualVersion = changeDataFeedHelper.getVersionsForAvailableCDC
        val expectedVersion = Some(17l, 18l)
        actualVersion should equal(expectedVersion)
        val actualDF = changeDataFeedHelper.readCDFIgnoreMissingCDC.get
        actualDF.select("_commit_version").distinct().count() should equal(2l)
      }
      describe("And then Disabled and Re-enabled again so") {
        it("CASE III - EDR : should not return provided versions and return actual queryable versions that work") {
          setUpForEDR("edr_snapshot", snapshotDF, path, updates, false)
          val actualVersions = ChangeDataFeedHelper(writePath, 0, 15).getAllVersionsWithCDFStatus
          actualVersions should equal(List((0, true), (1, true), (2, true), (3, true),
            (4, false), (5, false), (6, false),
            (7, true), (8, true),
            (9, false), (10, false), (11, false),
            (12, true), (13, true), (14, true), (15, true)))
        }
        it("CASE III - EDR : with a no op delete merge, should not return provided versions and return actual queryable versions that work") {
          val name = "edr_delete_snapshot"
          noOpDelete(name, setUpForEDR(name, snapshotDF, path, updates, true), updates.take(3), false)
          val changeDataFeedHelper = ChangeDataFeedHelper(writePath, 0, 30)
          val enabledVersions = changeDataFeedHelper.getRangesForCDFEnabledVersions
          enabledVersions.get should equal(List((0, 3), (7, 8), (12, 20)))
          val disabledVersions = changeDataFeedHelper.getRangesForCDFDisabledVersions
          disabledVersions.get should equal(List((4, 6), (9, 11), (21, 24)))
          val actualDF = changeDataFeedHelper.readCDFIgnoreMissingRangesForEDR.get
          actualDF.select("_commit_version").distinct().count() should equal(11l)
        }
      }
    }

    describe("should not return any versions when") {
      it("Case I - Delta Log is available but CDF is disabled between versions") {
        val name = "cdf_disabled_snapshot"
        val table = setUp(name, snapshotDF, path, updates.take(3))
        setCDF(name, false)
        executeMergeFor(name, table, updates.slice(4, 6))
        val cdfhEnd = ChangeDataFeedHelper(writePath, 0, 6)
        // Fails due to end version failure
        val actualOverall = cdfhEnd.getVersionsForAvailableDeltaLog
        val endDF = cdfhEnd.readCDFIgnoreMissingDeltaLog
        actualOverall should equal(None)
        endDF should equal(None)
        val cdfhStart = ChangeDataFeedHelper(writePath, 4, 6)
        // Fails due to start version failure
        val startVersionDisabled = cdfhStart.getVersionsForAvailableDeltaLog
        val startDF = cdfhStart.readCDFIgnoreMissingDeltaLog
        startVersionDisabled should equal(None)
        startDF should equal(None)
        val actual = ChangeDataFeedHelper(writePath, 0, 3)
        val actualWorkingVersions = actual.getVersionsForAvailableDeltaLog
        actualWorkingVersions should equal(Some(0, 3))
        val actualDF = actual.readCDFIgnoreMissingDeltaLog.get
        actualDF.select("_commit_version").distinct().count() should equal(4l)
      }
      it("Case II - All CDC data has been purged due to vacuum or deleted manually") {
        val name = "cdf_deleted_snapshot"
        val table = setUp(name, snapshotDF, path, updates.take(3))
        val changeDataDir = os.pwd / "tmp" / "delta-cdf-edr" / name / "_change_data"
        os.remove.all(changeDataDir)
        val mayBeVersion = ChangeDataFeedHelper(writePath, 0, 3).getVersionsForAvailableCDC
        mayBeVersion should equal(None)
      }
      it("Case II - CDC Data is deleted but CDF is disabled between versions so the check doesn't complete") {
        val name = "cdc_disabled_snapshot"
        val table = setUp(name, snapshotDF, path, updates.take(3))
        setCDF(name, false)
        executeMergeFor(name, table, updates.slice(4, 6))
        val changeDataDir = os.pwd / "tmp" / "delta-cdf-edr" / name / "_change_data"
        os.remove.all(changeDataDir)
        assertThrows[AssertionError] {
          ChangeDataFeedHelper(writePath, 0, 6).getVersionsForAvailableCDC
        }
      }
    }
  }

  def setCDF(tableName: String, flag: Boolean) = {
    //spark.sql("SHOW TBLPROPERTIES default.snapshot  ").show(false)
    spark.sql(s"ALTER TABLE default.${tableName} SET TBLPROPERTIES (delta.enableChangeDataFeed = $flag)")
    //spark.sql("SHOW TBLPROPERTIES default.snapshot  ").show(false)
  }

  def noOpDelete(tableName: String, deltaTable: DeltaTable, updates: List[(Int, String, Int)], lastOp: Boolean) = {
    import spark.implicits._
    val fakeDelete = Seq((1000, "Male", 35)).toDF("id", "gender", "age")
    deltaTable.as(tableName)
      .merge(fakeDelete.as("source"), s"${tableName}.id = source.id")
      .whenMatched()
      .delete()
      .execute()

    executeMergeFor(tableName, deltaTable, updates)
    setCDF(tableName, lastOp)
    executeMergeFor(tableName, deltaTable, updates)
  }

  def setUp(tableName: String, snapshotDF: DataFrame, path: String, updates: List[(Int, String, Int)]) = {
    writePath = path + "/" + tableName
    snapshotDF.write.format("delta").save(writePath)
    val table = DeltaTable.forPath(writePath)
    spark.sql(s"CREATE TABLE default.${tableName} USING DELTA LOCATION  '${writePath}' ")
    executeMergeFor(tableName, table, updates)
  }

  def setUpForEDR(tableName: String, snapshotDF: DataFrame, path: String, updates: List[(Int, String, Int)], lastOp: Boolean) = {
    val table = setUp(tableName, snapshotDF, path, updates.take(3))
    setCDF(tableName, false)
    executeMergeFor(tableName, table, updates.slice(4, 6))
    setCDF(tableName, true)
    executeMergeFor(tableName, table, updates.slice(7, 8))
    setCDF(tableName, false)
    executeMergeFor(tableName, table, updates.slice(9, 11))
    setCDF(tableName, true)
    executeMergeFor(tableName, table, updates.slice(12, 15))
    setCDF(tableName, lastOp)
    table
  }

  def executeMergeFor(tableName: String, deltaTable: DeltaTable, updates: List[(Int, String, Int)]) = {
    import spark.implicits._
    updates.foreach(row => {
      val dataFrame = Seq(row).toDF("id", "gender", "age")
      deltaTable.as(tableName)
        .merge(dataFrame.as("source"), s"${tableName}.id = source.id")
        .whenMatched
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    })
    deltaTable
  }
}