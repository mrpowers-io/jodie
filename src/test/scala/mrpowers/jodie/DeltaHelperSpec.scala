package mrpowers.jodie

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import io.delta.tables.DeltaTable
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.{BeforeAndAfterEach, FunSpec}

class DeltaHelperSpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer with BeforeAndAfterEach {

  override def afterEach(): Unit = {
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
      val exceptionMessage = intercept[NoSuchElementException]{
        DeltaHelpers.removeDuplicateRecords(deltaTable, duplicateColumns)
      }.getMessage

      assert(exceptionMessage.contains("the input parameter duplicateColumns must not be empty"))
    }

    it("should fail to remove duplicate when duplicateColumns does not exist in table") {
      val path = (os.pwd / "tmp" / "delta-duplicate-not-in-table").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson"),
      ).toDF("id", "firstname", "lastname")
      df.write.format("delta").mode("overwrite").save(path)
      val deltaTable = DeltaTable.forPath(path)
      val duplicateColumns = Seq("firstname","name")
      val exceptionMessage = intercept[JodieValidationError] {
        DeltaHelpers.removeDuplicateRecords(deltaTable, duplicateColumns)
      }.getMessage

      val tableColumns = deltaTable.toDF.columns.toSeq
      val diff = duplicateColumns.diff(tableColumns)
      assert(exceptionMessage.contains(s"these columns: $diff do not exists in the dataframe: $tableColumns"))
    }
  }

  describe("remove duplicate records from delta table using primary key") {
    it("should remove duplicates given a primary key and duplicate columns") {
      val path = (os.pwd / "tmp" / "delta-duplicate-pk").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson"),
        (5, "Jose", "Travolta"),
        (6, "Jose", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")
      df.write.format("delta").mode("overwrite").save(path)

      val deltaTable = DeltaTable.forPath(path)
      val duplicateColumns = Seq("lastname")
      val primaryKey = "id"
      DeltaHelpers.removeDuplicateRecords(deltaTable,primaryKey,duplicateColumns)
      val resultTable = spark.read.format("delta").load(path)
      val expectedTable = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (7, "Maria", "Pitt"),
      ).toDF("id", "firstname", "lastname")
      assertSmallDataFrameEquality(resultTable,expectedTable,orderedComparison = false,ignoreNullable = true)
    }

    it("should remove duplicates when not duplicate columns is provided") {
      val path = (os.pwd / "tmp" / "delta-pk-not-duplicate-columns").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson"),
        (5, "Jose", "Travolta"),
        (6, "Jose", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")
      df.write.format("delta").mode("overwrite").save(path)

      val deltaTable = DeltaTable.forPath(path)
      val primaryKey = "id"
      DeltaHelpers.removeDuplicateRecords(deltaTable, primaryKey)
      val resultTable = spark.read.format("delta").load(path)
      val expectedTable = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson"),
        (5, "Jose", "Travolta"),
        (6, "Jose", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")
      assertSmallDataFrameEquality(resultTable, expectedTable, orderedComparison = false, ignoreNullable = true)
    }

    it("should execute successful when delta table is empty") {
      val path = (os.pwd / "tmp" / "delta-duplicate-empty-table").toString()
      val df = spark.createDF(List(), List(
        ("id", IntegerType, true),
        ("firstname", StringType, true),
        ("lastname", StringType, true)),
      )
      df.write.format("delta").mode("overwrite").save(path)
      val deltaTable = DeltaTable.forPath(path)
      val duplicateColumns = Seq("firstname", "lastname")
      val primaryKey = "id"
      DeltaHelpers.removeDuplicateRecords(deltaTable, primaryKey, duplicateColumns)
      val resultTable = spark.read.format("delta").load(path)
      val expectedTable = spark.createDF(List(), List(
        ("id", IntegerType, true),
        ("firstname", StringType, true),
        ("lastname", StringType, true)),
      )
      assertSmallDataFrameEquality(resultTable, expectedTable, orderedComparison = false, ignoreNullable = true)
    }

    it("should fail to remove duplicate when not primary key is provided") {
      val path = (os.pwd / "tmp" / "delta-duplicate-no-pk").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson"),
        (5, "Jose", "Travolta"),
        (6, "Jose", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")
      df.write.format("delta").mode("overwrite").save(path)

      val deltaTable = DeltaTable.forPath(path)
      val primaryKey = ""
      val exceptionMessage = intercept[NoSuchElementException]{
        DeltaHelpers.removeDuplicateRecords(deltaTable, primaryKey)
      }.getMessage

      assert(exceptionMessage.contains("the input parameter primaryKey must not be empty"))
    }

    it("should fail to remove duplicate when duplicateColumns does not exist in table") {
      val path = (os.pwd / "tmp" / "delta-duplicate-cols-no-exists").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson"),
        (5, "Jose", "Travolta"),
        (6, "Jose", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")
      df.write.format("delta").mode("overwrite").save(path)

      val deltaTable = DeltaTable.forPath(path)
      val primaryKey = "id"
      val duplicateColumns = Seq("name","lastname")
      val exceptionMessage = intercept[JodieValidationError] {
        DeltaHelpers.removeDuplicateRecords(deltaTable, primaryKey,duplicateColumns)
      }.getMessage

      val tableColumns = deltaTable.toDF.columns.toSeq
      val diff = duplicateColumns.diff(tableColumns)
      assert(exceptionMessage.contains(s"these columns: $diff do not exists in the dataframe: $tableColumns"))
    }
  }
}
