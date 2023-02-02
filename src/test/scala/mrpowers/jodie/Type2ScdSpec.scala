package mrpowers.jodie

import org.scalatest.BeforeAndAfterEach
import java.sql.{Date, Timestamp}
import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import io.delta.tables.DeltaTable
import org.apache.spark.sql.types._
import org.scalatest.funspec.AnyFunSpec

class Type2ScdSpec
    extends AnyFunSpec
    with SparkSessionTestWrapper
    with BeforeAndAfterEach
    with DataFrameComparer {

  import spark.implicits._

  override def afterEach(): Unit = {
    val tmpDir = os.pwd / "tmp"
    os.remove.all(tmpDir)
  }

  describe("upsert") {
    it("upserts a Delta Lake with a single attribute") {
      val path = (os.pwd / "tmp" / "delta-upsert").toString()
      // create Delta Lake
      val df = spark.createDF(
        List(
          (1, "A", true, Timestamp.valueOf("2019-01-01 00:00:00"), null),
          (2, "B", true, Timestamp.valueOf("2019-01-01 00:00:00"), null),
          (4, "D", true, Timestamp.valueOf("2019-01-01 00:00:00"), null)
        ),
        List(
          ("pkey", IntegerType, true),
          ("attr", StringType, true),
          ("is_current", BooleanType, true),
          ("effective_time", TimestampType, true),
          ("end_time", TimestampType, true)
        )
      )
      df.write.format("delta").save(path)
      // create updates DF
      val updatesDF = Seq(
        (2, "Z", Timestamp.valueOf("2020-01-01 00:00:00")), // value to upsert
        (3, "C", Timestamp.valueOf("2020-09-15 00:00:00"))  // new value
      ).toDF("pkey", "attr", "effective_time")
      // perform upsert
      val table = DeltaTable.forPath(path)
      Type2Scd.upsert(table, updatesDF, "pkey", Seq("attr"))
      // show result
      val res = spark.read.format("delta").load(path)
      val expected = Seq(
        (
          2,
          "B",
          false,
          Timestamp.valueOf("2019-01-01 00:00:00"),
          Timestamp.valueOf("2020-01-01 00:00:00")
        ),
        (3, "C", true, Timestamp.valueOf("2020-09-15 00:00:00"), null),
        (2, "Z", true, Timestamp.valueOf("2020-01-01 00:00:00"), null),
        (4, "D", true, Timestamp.valueOf("2019-01-01 00:00:00"), null),
        (1, "A", true, Timestamp.valueOf("2019-01-01 00:00:00"), null)
      ).toDF("pkey", "attr", "is_current", "effective_time", "end_time")
      assertSmallDataFrameEquality(res, expected, orderedComparison = false, ignoreNullable = true)
    }

    it("errors out if the base DataFrame doesn't contain all the required columns") {
      val path = (os.pwd / "tmp" / "delta-upsert-err").toString()
      // create Delta Lake
      val df = spark.createDF(
        List(
          (true, Timestamp.valueOf("2019-01-01 00:00:00"), null),
          (true, Timestamp.valueOf("2019-01-01 00:00:00"), null),
          (true, Timestamp.valueOf("2019-01-01 00:00:00"), null)
        ),
        List(
          ("is_current", BooleanType, true),
          ("effective_time", TimestampType, true),
          ("end_time", TimestampType, true)
        )
      )
      df.write.format("delta").save(path)
      // create updates DF
      val updatesDF = Seq(
        (2, "Z", Timestamp.valueOf("2020-01-01 00:00:00")), // value to upsert
        (3, "C", Timestamp.valueOf("2020-09-15 00:00:00"))  // new value
      ).toDF("pkey", "attr", "effective_time")
      // perform upsert
      intercept[JodieValidationError] {
        val table = DeltaTable.forPath(path)
        Type2Scd.upsert(table, updatesDF, "pkey", Seq("attr"))
      }
    }

    it("errors out if the updates table doesn't contain all the required columns") {
      val path = (os.pwd / "tmp" / "delta-upsert-err2").toString()
      // create Delta Lake
      val df = spark.createDF(
        List(
          (1, "A", true, Timestamp.valueOf("2019-01-01 00:00:00"), null),
          (2, "B", true, Timestamp.valueOf("2019-01-01 00:00:00"), null),
          (4, "D", true, Timestamp.valueOf("2019-01-01 00:00:00"), null)
        ),
        List(
          ("pkey", IntegerType, true),
          ("attr", StringType, true),
          ("is_current", BooleanType, true),
          ("effective_time", TimestampType, true),
          ("end_time", TimestampType, true)
        )
      )
      df.write.format("delta").save(path)
      // create updates DF
      val updatesDF = Seq(
        (2, "Z"), // value to upsert
        (3, "C")  // new value
      ).toDF("pkey", "attr")
      intercept[JodieValidationError] {
        val table = DeltaTable.forPath(path)
        Type2Scd.upsert(table, updatesDF, "pkey", Seq("attr"))
      }
    }

    it("upserts based on multiple attributes") {
      val path = (os.pwd / "tmp" / "delta-upsert2").toString()
      // create Delta Lake
      val df = spark.createDF(
        List(
          (1, "A", "A", true, Timestamp.valueOf("2019-01-01 00:00:00"), null),
          (2, "B", "B", true, Timestamp.valueOf("2019-01-01 00:00:00"), null),
          (4, "D", "D", true, Timestamp.valueOf("2019-01-01 00:00:00"), null)
        ),
        List(
          ("pkey", IntegerType, true),
          ("attr1", StringType, true),
          ("attr2", StringType, true),
          ("is_current", BooleanType, true),
          ("effective_time", TimestampType, true),
          ("end_time", TimestampType, true)
        )
      )
      df.write.format("delta").save(path)
      // create updates DF
      val updatesDF = Seq(
        (2, "Z", null, Timestamp.valueOf("2020-01-01 00:00:00")), // value to upsert
        (3, "C", "C", Timestamp.valueOf("2020-09-15 00:00:00"))   // new value
      ).toDF("pkey", "attr1", "attr2", "effective_time")
      // perform upsert
      val table = DeltaTable.forPath(path)
      Type2Scd.upsert(table, updatesDF, "pkey", Seq("attr1", "attr2"))
      val res = spark.read.format("delta").load(path)
      val expected = Seq(
        (
          2,
          "B",
          "B",
          false,
          Timestamp.valueOf("2019-01-01 00:00:00"),
          Timestamp.valueOf("2020-01-01 00:00:00")
        ),
        (3, "C", "C", true, Timestamp.valueOf("2020-09-15 00:00:00"), null),
        (1, "A", "A", true, Timestamp.valueOf("2019-01-01 00:00:00"), null),
        (4, "D", "D", true, Timestamp.valueOf("2019-01-01 00:00:00"), null),
        (2, "Z", null, true, Timestamp.valueOf("2020-01-01 00:00:00"), null)
      ).toDF("pkey", "attr1", "attr2", "is_current", "effective_time", "end_time")
      assertSmallDataFrameEquality(res, expected, orderedComparison = false, ignoreNullable = true)
    }
  }

  describe("genericUpsert") {
    it("upserts based on date columns") {
      val path = (os.pwd / "tmp" / "delta-upsert-date").toString()
      // create Delta Lake
      val df = spark.createDF(
        List(
          (1, "A", true, Date.valueOf("2019-01-01"), null),
          (2, "B", true, Date.valueOf("2019-01-01"), null),
          (4, "D", true, Date.valueOf("2019-01-01"), null)
        ),
        List(
          ("pkey", IntegerType, true),
          ("attr", StringType, true),
          ("cur", BooleanType, true),
          ("effective_date", DateType, true),
          ("end_date", DateType, true)
        )
      )
      df.write.format("delta").save(path)
      // create updates DF
      val updatesDF = Seq(
        (2, "Z", Date.valueOf("2020-01-01")), // value to upsert
        (3, "C", Date.valueOf("2020-09-15"))  // new value
      ).toDF("pkey", "attr", "effective_date")
      // perform upsert
      val table = DeltaTable.forPath(path)
      Type2Scd.genericUpsert(
        table,
        updatesDF,
        "pkey",
        Seq("attr"),
        "cur",
        "effective_date",
        "end_date"
      )
      val res = spark.read.format("delta").load(path)
      val expected = Seq(
        (2, "B", false, Date.valueOf("2019-01-01"), Date.valueOf("2020-01-01")),
        (3, "C", true, Date.valueOf("2020-09-15"), null),
        (2, "Z", true, Date.valueOf("2020-01-01"), null),
        (4, "D", true, Date.valueOf("2019-01-01"), null),
        (1, "A", true, Date.valueOf("2019-01-01"), null)
      ).toDF("pkey", "attr", "cur", "effective_date", "end_date")
      assertSmallDataFrameEquality(res, expected, orderedComparison = false, ignoreNullable = true)
    }

    it("upserts based on version number") {
      val path = (os.pwd / "tmp" / "delta-upsert-ver").toString()
      // create Delta Lake
      val df = Seq(
        (1, "A", true, 1, null),
        (2, "B", true, 1, null),
        (4, "D", true, 1, null)
      ).toDF("pkey", "attr", "is_current", "effective_ver", "end_ver")
        .withColumn("end_ver", $"end_ver".cast(IntegerType))
      df.write.format("delta").save(path)
      // create updates DF
      val updatesDF = Seq(
        (2, "Z", 2), // value to upsert
        (3, "C", 3)  // new value
      ).toDF("pkey", "attr", "effective_ver")
      // perform upsert
      val table = DeltaTable.forPath(path)
      Type2Scd.genericUpsert(
        table,
        updatesDF,
        "pkey",
        Seq("attr"),
        "is_current",
        "effective_ver",
        "end_ver"
      )
      // show result
      val res = spark.read.format("delta").load(path)
      val expected = spark.createDF(
        List(
          (2, "B", false, 1, 2),
          (3, "C", true, 3, null),
          (2, "Z", true, 2, null),
          (4, "D", true, 1, null),
          (1, "A", true, 1, null)
        ),
        List(
          ("pkey", IntegerType, true),
          ("attr", StringType, true),
          ("is_current", BooleanType, true),
          ("effective_ver", IntegerType, true),
          ("end_ver", IntegerType, true)
        )
      )
      assertSmallDataFrameEquality(res, expected, orderedComparison = false, ignoreNullable = true)
    }
  }

}
