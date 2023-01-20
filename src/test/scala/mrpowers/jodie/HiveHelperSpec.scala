package mrpowers.jodie

import mrpowers.jodie.HiveHelpers.HiveTableType
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSpec}

class HiveHelperSpec extends FunSpec with SparkSessionTestWrapper with BeforeAndAfterEach{
  import spark.implicits._
    override def afterEach():Unit = {
      val tmpDir = os.pwd / "tmp"
      os.remove.all(tmpDir)
      SparkSession.active.sql("drop table IF EXISTS num_table")
    }

    describe("Hive table types"){
      it("should return table type managed"){

        val df = List("1","2","3").toDF
        val tableName = "num_table"
        df.write.saveAsTable(tableName)
        val result = HiveHelpers.getTableType(tableName)
        val expected = HiveTableType.MANAGED
        assertResult(expected)(result)
      }

      it("should return table type external"){
        val df = List("1","2","3").toDF
        val tableName = "num_table"
        val tmpDir = os.pwd / "tmp"
        val dataDir =  tmpDir / tableName / ".parquet"
        df.write.save(tmpDir.toString)
        SparkSession.active.sql(s"CREATE EXTERNAL TABLE num_table(value string) STORED AS PARQUET LOCATION '$dataDir'")
        val result = HiveHelpers.getTableType(tableName)
        val expected = HiveTableType.EXTERNAL
        assertResult(expected)(result)
      }

      it("should return table type non-registered") {
        val tableName = "num_table"
        val result = HiveHelpers.getTableType(tableName)
        val expected = HiveTableType.NONREGISTERED
        assertResult(expected)(result)
      }

      it("should be able to recognize a managed delta table"){
        val df = List("1", "2", "3").toDF
        val tableName = "num_table"
        df.write
          .format("delta")
          .saveAsTable(tableName)
        val result = HiveHelpers.getTableType(tableName)
        val expected = HiveTableType.MANAGED
        assertResult(expected)(result)
      }

      it("should be able to recognize an external delta table") {
        val df = List("1", "2", "3").toDF
        val tableName = "num_table"
        val tmpDir = os.pwd / "tmp"
        df.write
          .format("delta")
          .option("path",tmpDir.toString())
          .saveAsTable(tableName)
        val result = HiveHelpers.getTableType(tableName)
        val expected = HiveTableType.EXTERNAL
        assertResult(expected)(result)
      }

      it("should be able to recognize an non-registered delta table"){
        val df = List("1", "2", "3").toDF
        val tmpDir = os.pwd / "tmp"
        val tableName = "num_table"
        df.write
          .format("delta")
          .save((tmpDir / tableName).toString())
        val result = HiveHelpers.getTableType(tableName)
        val expected = HiveTableType.NONREGISTERED
        assertResult(expected)(result)
      }
    }
}
