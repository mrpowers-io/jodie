package mrpowers.jodie

import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, DatasetContentMismatch}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class OperationMetricHelperSpec
    extends AnyFunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer
    with BeforeAndAfterEach {
  var writePath = ""
  override def afterEach(): Unit = {
    val tmpDir = os.pwd / "tmp" / "delta-opm"
    os.remove.all(tmpDir)
  }

  describe("When Delta Table has relevant operation metric") {
    val rows    = Seq((1, "Male", 25), (2, "Female", 35), (3, "Female", 45), (4, "Male", 18))
    val updates = List((1, "Male", 35), (2, "Male", 100), (5, "Male", 101), (4, "Female", 18))
    val path    = (os.pwd / "tmp" / "delta-opm").toString()
    import spark.implicits._
    val snapshotDF            = rows.toDF("id", "gender", "age")
    implicit val sparkSession = spark
    it("should return valid count metric") {
      val name = "snapshot"
      val deltaTable =
        DeltaTestUtils.executeMergeFor(
          name,
          createDeltaTable(name, snapshotDF, path, None),
          updates
        )
      deltaTable.delete("id == 5")
      Seq((10, "Female", 35))
        .toDF("id", "gender", "age")
        .write
        .format("delta")
        .mode("append")
        .save(writePath)
      val actualDF = OperationMetricHelper(writePath).getCountMetricsAsDF()
      val expected = toVersionDF(
        Seq(
          (6L, 0L, 1L, 0L, 1L),
          (5L, 1L, 0L, 0L, 0L),
          (4L, 0L, 0L, 1L, 1L),
          (3L, 0L, 1L, 0L, 1L),
          (2L, 0L, 0L, 1L, 1L),
          (1L, 0L, 0L, 1L, 1L),
          (0L, 0L, 4L, 0L, 4L)
        )
      )
      assertSmallDataFrameEquality(actualDF, expected)
    }
    it("should return valid metric for single partition column") {
      val deltaTable = partitionWithMerge("partitioned_snapshot", rows, updates, path)
      val actual = OperationMetricHelper(writePath).getCountMetricsAsDF(Some(" country = 'USA'"))
      val versions: Array[Row] = getCountryVersions(deltaTable)
      val expected =
        toVersionDF(
          Seq(
            (versions.head.getAs[Long]("version"), 0L, 0L, 1L, 1L),
            (versions.tail.head.getAs[Long]("version"), 0L, 0L, 1L, 1L),
            (0L, 0L, 2L, 0L, 2L)
          )
        )
      assertSmallDataFrameEquality(actual, expected)
    }
    it("should return valid metric for single partition column containing deletes and appends") {
      val deltaTable = partitionWithMerge("single_partitioned_snapshot", rows, updates, path)
      deltaTable.delete("country == 'USA' and age == 100")
      Seq((10, "Female", 35, "USA"))
        .toDF("id", "gender", "age", "country")
        .write
        .format("delta")
        .mode("append")
        .partitionBy("country")
        .save(writePath)
      val condition = " country = 'USA'"
      val actual    = OperationMetricHelper(writePath).getCountMetricsAsDF(Some(" country = 'USA'"))
      val versions: Array[Row] = getCountryVersions(deltaTable)
      val expected =
        toVersionDF(
          Seq(
            (6L, 0L, 1L, 0, 1L),
            (5L, 1L, 0L, 0L, 0L),
            (versions.head.getAs[Long]("version"), 0L, 0L, 1L, 1L),
            (versions.tail.head.getAs[Long]("version"), 0L, 0L, 1L, 1L),
            (0L, 0L, 2L, 0L, 2L)
          )
        )
      assertSmallDataFrameEquality(actual, expected)
      // Query works without single quotes
      val actualWithoutSingleQuote =
        OperationMetricHelper(writePath).getCountMetricsAsDF(Some(" country = USA"))
      assertSmallDataFrameEquality(actualWithoutSingleQuote, expected)
      intercept[DatasetContentMismatch] {
        // This query does not work because partition has country=USA and query passed is country=usa
        // Query types that would work but return wrong count metric, more precisely 0L(zero) as count for write metric
        val actualDoesNotMatchPartitionCase =
          OperationMetricHelper(writePath).getCountMetricsAsDF(Some(" country = usa"))
        assertSmallDataFrameEquality(actualDoesNotMatchPartitionCase, expected)
      }
    }
    it("should return valid metric for multiple partition columns") {
      val deltaTable = multiplePartitionWithMerge("multi_partitioned_snapshot", rows, updates, path)
      deltaTable.delete("country == 'USA' and gender = 'Female' and id == 2")
      Seq((10, "Female", 35, "USA"))
        .toDF("id", "gender", "age", "country")
        .write
        .format("delta")
        .mode("append")
        .partitionBy("country", "gender")
        .save(writePath)
      val actual = OperationMetricHelper(writePath).getCountMetricsAsDF(
        Some(" country = 'USA' and gender = 'Female'")
      )
      val version = getMergeVersionForPartition(deltaTable)
      val expected =
        toVersionDF(
          Seq(
            (6L, 0L, 1L, 0L, 1L),
            (5L, 1L, 0L, 0L, 0L),
            (version, 0L, 1L, 0L, 1L),
            (0L, 0L, 1L, 0L, 1L)
          )
        )
      assertSmallDataFrameEquality(actual, expected)
      val actualWithoutSingleQuote = OperationMetricHelper(writePath).getCountMetricsAsDF(
        Some(" country = USA and gender = Female")
      )
      assertSmallDataFrameEquality(actualWithoutSingleQuote, expected)
      intercept[DatasetContentMismatch] {
        val actualDoesNotMatchPartitionCase = OperationMetricHelper(writePath).getCountMetricsAsDF(
          Some(" country = usa and gender = female ")
        )
        assertSmallDataFrameEquality(actualDoesNotMatchPartitionCase, expected)
      }
    }
    it("should return valid metric for multiple partition columns containing updates") {
      val deltaTable =
        multiplePartitionWithMerge("multi_partitioned_snapshot_updated", rows, updates, path)
      deltaTable.delete("country == 'USA' and gender = 'Female' and id == 2")
      Seq((10, "Female", 35, "USA"))
        .toDF("id", "gender", "age", "country")
        .write
        .format("delta")
        .mode("append")
        .partitionBy("country", "gender")
        .save(writePath)
      spark.sql(s"ALTER TABLE default.multi_partitioned_snapshot_updated SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

      deltaTable.updateExpr(
        "country == 'USA' and gender = 'Female' and id == 4",
        Map("age" -> "533")
      )
      deltaTable.optimize().where("country='USA' and gender='Female'").executeCompaction()
      val actual = OperationMetricHelper(writePath).getCountMetricsAsDF(
        Some(" country = 'USA' and gender = 'Female'")
      )
      val version = getMergeVersionForPartition(deltaTable)
      val expected =
        toVersionDF(
          Seq(
            (8L, 0L, 0L, 1L, 0L),
            (6L, 0L, 1L, 0L, 1L),
            (5L, 1L, 0L, 0L, 0L),
            (version, 0L, 1L, 0L, 1L),
            (0L, 0L, 1L, 0L, 1L)
          )
        )
      assertSmallDataFrameEquality(actual, expected)
    }
  }

  private def getMergeVersionForPartition(deltaTable: DeltaTable): Long = {
    val versionDF = deltaTable
      .history()
      .filter(" version > 0 and version < 5")
      .select("version", "operationParameters.predicate")
      .filter("predicate like '%USA%' and predicate like '%Female%'")
    assert(versionDF.count() == 1)
    versionDF.take(1).head.getAs[Long]("version")
  }

  private def getCountryVersions(deltaTable: DeltaTable) = {
    val versionDF = deltaTable
      .history()
      .filter("operation == 'MERGE'")
      .select("version", "operationParameters.predicate")
      .filter("predicate like '%USA%'")
      .orderBy(desc("version"))
    assert(versionDF.count() == 2)
    val versions = versionDF.take(2)
    versions
  }

  private def partitionWithMerge(
      tableName: String,
      rows: Seq[(Int, String, Int)],
      updates: List[(Int, String, Int)],
      path: String
  ): DeltaTable = {
    import spark.implicits._
    val rowsWithCountry = rows.map(x => appendCountry(x))
    val deltaTable = createDeltaTable(
      tableName,
      rowsWithCountry.toDF("id", "gender", "age", "country"),
      path,
      Some(Seq("country"))
    )
    val upsertCandidates = updates.map(x => appendCountry(x))
    upsertCandidates
      .groupBy(x => x._4)
      .foreach(y => {
        DeltaTestUtils.executeMergeWithReducedSearchSpace(
          tableName,
          deltaTable,
          y._2,
          s" ${tableName}.country == '${y._1}'"
        )
        ()
      })
    deltaTable
  }

  private def multiplePartitionWithMerge(
      tableName: String,
      rows: Seq[(Int, String, Int)],
      updates: List[(Int, String, Int)],
      path: String
  ): DeltaTable = {
    import spark.implicits._
    val rowsWithCountry = rows.map(x => appendCountry(x))
    val deltaTable = createDeltaTable(
      tableName,
      rowsWithCountry.toDF("id", "gender", "age", "country"),
      path,
      Some(Seq("country", "gender"))
    )
    val upsertCandidates = updates.map(x => appendCountry(x))
    upsertCandidates
      .groupBy(x => (x._4, x._2))
      .foreach(y => {
        DeltaTestUtils.executeMergeWithReducedSearchSpace(
          tableName,
          deltaTable,
          y._2,
          s" ${tableName}.country == '${y._1._1}' and ${tableName}.gender == '${y._1._2}'"
        )
        ()
      })
    deltaTable
  }

  private def toVersionDF(s: Seq[(Long, Long, Long, Long, Long)]): DataFrame = {
    import spark.implicits._
    s.toDF(
      "version",
      "deleted",
      "inserted",
      "updated",
      "source_rows"
    )
  }

  private def appendCountry(x: (Int, String, Int)) = {
    if (x._1 % 2 == 0) (x._1, x._2, x._3, "USA") else (x._1, x._2, x._3, "IND")
  }

  def createDeltaTable(
      tableName: String,
      snapshotDF: DataFrame,
      path: String,
      partitionColumn: Option[Seq[String]]
  ) = {
    writePath = path + "/" + tableName
    partitionColumn match {
      case None => snapshotDF.write.format("delta").save(writePath)
      case Some(pc) =>
        snapshotDF.write
          .format("delta")
          .partitionBy(pc: _*)
          .save(writePath)
    }
    spark.sql(s"CREATE TABLE default.${tableName} USING DELTA LOCATION  '${writePath}' ")
    DeltaTable.forPath(writePath)
  }

}
