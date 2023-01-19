package mrpowers.jodie

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterEach, FunSpec}

class HiveHelperSpec extends FunSpec with SparkSessionTestWrapper with BeforeAndAfterEach{

    override def afterEach():Unit = {
      val tmpDir = os.pwd / "tmp"
      os.remove.all(tmpDir)
      SparkSession.active.sql("drop table IF EXISTS num_table")
    }

    describe("Hive table types"){
      it("should return table type managed"){
        import spark.implicits._
        val df = List("1","2","3").toDF
        val tableName = "num_table"
        df.write.saveAsTable(tableName)
        val result = HiveViewHelpers.getTableType(tableName)
        val expected = "MANAGED"
        assertResult(expected)(result.label)
      }

      it("should return table type external"){
        import spark.implicits._
        val df = List("1","2","3").toDF
        val tableName = "num_table"
        val tmpDir = os.pwd / "tmp"
        val dataDir =  tmpDir / tableName / ".parquet"
        df.write.save(tmpDir.toString)
        SparkSession.active.sql(s"CREATE EXTERNAL TABLE num_table(value string) STORED AS PARQUET LOCATION '$dataDir'")
        val result = HiveViewHelpers.getTableType(tableName)
        val expected = "EXTERNAL"
        assertResult(expected)(result.label)
      }

      it("should return table type non-registered") {
        val tableName = "num_table"
        val result = HiveViewHelpers.getTableType(tableName)
        val expected = "NONREGISTERED"
        assertResult(expected)(result.label)
      }
    }
}
