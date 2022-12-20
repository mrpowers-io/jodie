package mrpowers.jodie

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import io.delta.tables.DeltaTable
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.{BeforeAndAfterEach, FunSpec}

class DeltaHelperSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer with BeforeAndAfterEach {

  override def afterEach() {
    val tmpDir = os.pwd / "tmp"
    os.remove.all(tmpDir)
  }

  import spark.implicits._

  describe("remove duplicate records from delta table"){
    it("should remove duplicates successful"){
      val path = (os.pwd / "tmp" / "delta-duplicate").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson"),
        (5, "Jose", "Travolta"),
        (6, "Jose", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id","firstname","lastname")
      df.write.format("delta").mode("overwrite").save(path)

      val deltaTable = DeltaTable.forPath(path)
      val duplicateColumns = Seq("firstname","lastname")
      DeltaHelpers.removeDuplicateRecords(deltaTable,duplicateColumns)

      val resultTable = spark.read.format("delta").load(path)
      val expectedTable = Seq(
        (2, "Maria", "Willis"),
        (7, "Maria", "Pitt"),
      ).toDF("id", "firstname", "lastname")

      assertSmallDataFrameEquality(resultTable, expectedTable, orderedComparison = false, ignoreNullable = true)
    }

    it("should execute successful when applied into an empty table"){
      val path = (os.pwd / "tmp" / "delta-duplicate-empty-table").toString()
      val df = spark.createDF(List(),List(
        ("id",IntegerType,true),
        ("firstname",StringType,true),
        ("lastname",StringType,true)),
      )
      df.write.format("delta").mode("overwrite").save(path)
      val deltaTable = DeltaTable.forPath(path)
      val duplicateColumns = Seq("firstname","lastname")
      DeltaHelpers.removeDuplicateRecords(deltaTable, duplicateColumns)
      val resultTable = spark.read.format("delta").load(path)
      val expectedTable = spark.createDF(List(), List(
        ("id", IntegerType, true),
        ("firstname", StringType, true),
        ("lastname", StringType, true)),
      )
      assertSmallDataFrameEquality(resultTable,expectedTable,orderedComparison = false,ignoreNullable = true)
    }

    it("should fail to remove duplicate when duplicateColumns is empty"){
      val path = (os.pwd / "tmp" / "delta-duplicate-empty-list").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson"),
      ).toDF("id", "firstname", "lastname")
      df.write.format("delta").mode("overwrite").save(path)
      val deltaTable = DeltaTable.forPath(path)
      val duplicateColumns = Seq()
      val e = intercept[NoSuchElementException]{
        DeltaHelpers.removeDuplicateRecords(deltaTable, duplicateColumns)
      }.getMessage

      assert(e.contains("the input parameter duplicateColumns could not be empty"))
    }

    it("should fail to remove duplicate when duplicateColumns does not exist in table") {
      val path = (os.pwd / "tmp" / "delta-duplicate-empty-list").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson"),
      ).toDF("id", "firstname", "lastname")
      df.write.format("delta").mode("overwrite").save(path)
      val deltaTable = DeltaTable.forPath(path)
      val duplicateColumns = Seq("firstname","name")
      val e = intercept[JodieValidationError] {
        DeltaHelpers.removeDuplicateRecords(deltaTable, duplicateColumns)
      }.getMessage

      assert(e.contains(s"there are columns in duplicateColumns:$duplicateColumns that do not exists in the deltaTable: ${deltaTable.toDF.columns.toSeq}"))
    }
  }
}
