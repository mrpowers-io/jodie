package mrpowers.jodie

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import io.delta.tables.DeltaTable
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class DeltaHelperSpec
    extends AnyFunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer
    with BeforeAndAfterEach {

  override def afterEach(): Unit = {
    val tmpDir = os.pwd / "tmp"
    os.remove.all(tmpDir)
  }

  import spark.implicits._

  describe("remove duplicate records from delta table") {
    it("should remove duplicates successful") {
      val path = (os.pwd / "tmp" / "delta-duplicate").toString()
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

      val deltaTable       = DeltaTable.forPath(path)
      val duplicateColumns = Seq("firstname", "lastname")
      DeltaHelpers.killDuplicateRecords(deltaTable, duplicateColumns)

      val resultTable = spark.read.format("delta").load(path)
      val expectedTable = Seq(
        (2, "Maria", "Willis"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")

      assertSmallDataFrameEquality(
        resultTable,
        expectedTable,
        orderedComparison = false,
        ignoreNullable = true
      )
    }

    it("should execute successful when applied into an empty table") {
      val path = (os.pwd / "tmp" / "delta-duplicate-empty-table").toString()
      val df = spark.createDF(
        List(),
        List(
          ("id", IntegerType, true),
          ("firstname", StringType, true),
          ("lastname", StringType, true)
        )
      )
      df.write.format("delta").mode("overwrite").save(path)
      val deltaTable       = DeltaTable.forPath(path)
      val duplicateColumns = Seq("firstname", "lastname")
      DeltaHelpers.killDuplicateRecords(deltaTable, duplicateColumns)
      val resultTable = spark.read.format("delta").load(path)
      val expectedTable = spark.createDF(
        List(),
        List(
          ("id", IntegerType, true),
          ("firstname", StringType, true),
          ("lastname", StringType, true)
        )
      )
      assertSmallDataFrameEquality(
        resultTable,
        expectedTable,
        orderedComparison = false,
        ignoreNullable = true
      )
    }

    it("should fail to remove duplicate when duplicateColumns is empty") {
      val path = (os.pwd / "tmp" / "delta-duplicate-empty-list").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson")
      ).toDF("id", "firstname", "lastname")
      df.write.format("delta").mode("overwrite").save(path)
      val deltaTable       = DeltaTable.forPath(path)
      val duplicateColumns = Seq()
      val exceptionMessage = intercept[NoSuchElementException] {
        DeltaHelpers.killDuplicateRecords(deltaTable, duplicateColumns)
      }.getMessage

      assert(exceptionMessage.contains("the input parameter duplicateColumns must not be empty"))
    }

    it("should fail to remove duplicate when duplicateColumns does not exist in table") {
      val path = (os.pwd / "tmp" / "delta-duplicate-not-in-table").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson")
      ).toDF("id", "firstname", "lastname")
      df.write.format("delta").mode("overwrite").save(path)
      val deltaTable       = DeltaTable.forPath(path)
      val duplicateColumns = Seq("firstname", "name")
      val exceptionMessage = intercept[JodieValidationError] {
        DeltaHelpers.killDuplicateRecords(deltaTable, duplicateColumns)
      }.getMessage

      val tableColumns = deltaTable.toDF.columns.toSeq
      val diff         = duplicateColumns.diff(tableColumns)
      assert(
        exceptionMessage.contains(
          s"these columns: $diff do not exists in the dataframe: $tableColumns"
        )
      )
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

      val deltaTable       = DeltaTable.forPath(path)
      val duplicateColumns = Seq("lastname")
      val primaryKey       = "id"
      DeltaHelpers.removeDuplicateRecords(deltaTable, primaryKey, duplicateColumns)
      val resultTable = spark.read.format("delta").load(path)
      val expectedTable = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")
      assertSmallDataFrameEquality(
        resultTable,
        expectedTable,
        orderedComparison = false,
        ignoreNullable = true
      )
    }

    it("should fail to remove duplicates when not duplicate columns is provided") {
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

      val errorMessage = intercept[NoSuchElementException] {
        DeltaHelpers.removeDuplicateRecords(deltaTable, primaryKey, Seq())
      }.getMessage
      val expectedResult = "the input parameter duplicateColumns must not be empty"

      assertResult(expectedResult)(errorMessage)
    }

    it("should execute successful when delta table is empty") {
      val path = (os.pwd / "tmp" / "delta-duplicate-empty-table").toString()
      val df = spark.createDF(
        List(),
        List(
          ("id", IntegerType, true),
          ("firstname", StringType, true),
          ("lastname", StringType, true)
        )
      )
      df.write.format("delta").mode("overwrite").save(path)
      val deltaTable       = DeltaTable.forPath(path)
      val duplicateColumns = Seq("firstname", "lastname")
      val primaryKey       = "id"
      DeltaHelpers.removeDuplicateRecords(deltaTable, primaryKey, duplicateColumns)
      val resultTable = spark.read.format("delta").load(path)
      val expectedTable = spark.createDF(
        List(),
        List(
          ("id", IntegerType, true),
          ("firstname", StringType, true),
          ("lastname", StringType, true)
        )
      )
      assertSmallDataFrameEquality(
        resultTable,
        expectedTable,
        orderedComparison = false,
        ignoreNullable = true
      )
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
      val exceptionMessage = intercept[NoSuchElementException] {
        DeltaHelpers.removeDuplicateRecords(deltaTable, primaryKey, Seq("lastname"))
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

      val deltaTable       = DeltaTable.forPath(path)
      val primaryKey       = "id"
      val duplicateColumns = Seq("name", "lastname")
      val exceptionMessage = intercept[JodieValidationError] {
        DeltaHelpers.removeDuplicateRecords(deltaTable, primaryKey, duplicateColumns)
      }.getMessage

      val tableColumns = deltaTable.toDF.columns.toSeq
      val diff         = duplicateColumns.diff(tableColumns)
      assert(
        exceptionMessage.contains(
          s"these columns: $diff do not exists in the dataframe: $tableColumns"
        )
      )
    }
  }

  describe("drop duplicates from table give a set of unique cols") {
    it("should remove duplicate records from table") {
      val path = (os.pwd / "tmp" / "delta-duplicate-cols").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (1, "Benito", "Jackson"),
        (1, "Benito", "Jackson"),
        (1, "Benito", "Jackson"),
        (1, "Benito", "Jackson")
      ).toDF("id", "firstname", "lastname")
      df.write
        .format("delta")
        .mode("overwrite")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      DeltaHelpers.removeDuplicateRecords(deltaTable, Seq("id", "firstname", "lastname"))
      val resultTable = spark.read.format("delta").load(path)
      val expectedTable = spark.createDF(
        List((1, "Benito", "Jackson")),
        List(
          ("id", IntegerType, true),
          ("firstname", StringType, true),
          ("lastname", StringType, true)
        )
      )
      assertSmallDataFrameEquality(
        resultTable,
        expectedTable,
        orderedComparison = false,
        ignoreNullable = true
      )
    }

    it("should remove duplicate records from table using two columns") {
      val path = (os.pwd / "tmp" / "delta-duplicate-cols").toString()
      val df = Seq(
        (2, "Benito", "Jackson"),
        (1, "Benito", "Jackson"),
        (3, "Benito", "Jackson"),
        (4, "Benito", "Jackson"),
        (5, "Benito", "Jackson")
      ).toDF("id", "firstname", "lastname")
      df.write
        .format("delta")
        .mode("overwrite")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      DeltaHelpers.removeDuplicateRecords(deltaTable, Seq("firstname", "lastname"))
      val resultTable = spark.read.format("delta").load(path)
      val expectedTable = spark.createDF(
        List((2, "Benito", "Jackson")),
        List(
          ("id", IntegerType, true),
          ("firstname", StringType, true),
          ("lastname", StringType, true)
        )
      )
      assertSmallDataFrameEquality(
        resultTable,
        expectedTable,
        orderedComparison = false,
        ignoreNullable = true
      )
    }

    it("should fail to remove duplicate records when columns input parameter is empty") {
      val path = (os.pwd / "tmp" / "delta-duplicate-cols-fail").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Benito", "Jackson"),
        (3, "Benito", "Jackson"),
        (4, "Benito", "Jackson")
      ).toDF("id", "firstname", "lastname")
      df.write
        .format("delta")
        .mode("overwrite")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      val errorMessage = intercept[NoSuchElementException] {
        DeltaHelpers.removeDuplicateRecords(deltaTable, Nil)
      }.getMessage
      val expectedResult = "the input parameter duplicateColumns must not be empty"

      assertResult(expectedResult)(errorMessage)
    }

    it(
      "should fail to remove duplicate records when columns input parameter do not belong to the table"
    ) {
      val path = (os.pwd / "tmp" / "delta-duplicate-cols-fail").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Benito", "Jackson"),
        (3, "Benito", "Jackson"),
        (4, "Benito", "Jackson")
      ).toDF("id", "firstname", "lastname")
      df.write
        .format("delta")
        .mode("overwrite")
        .save(path)
      val deltaTable    = DeltaTable.forPath(path)
      val unknownColumn = "secondname"
      val errorMessage = intercept[JodieValidationError] {
        DeltaHelpers.removeDuplicateRecords(deltaTable, Seq("firstname", unknownColumn))
      }.getMessage
      val expectedResult =
        s"these columns: List($unknownColumn) do not exists in the dataframe: ${df.columns.mkString("WrappedArray(", ", ", ")")}"

      assertResult(expectedResult)(errorMessage)
    }
  }

  describe("get location path of a delta table") {
    it("should return the location path given a delta table") {
      val path = (os.pwd / "tmp" / "delta-location").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta")
      ).toDF("id", "firstname", "lastname")
      df.write.format("delta").mode("overwrite").save(path)
      val deltaTable = DeltaTable.forPath(path)
      val result = DeltaHelpers.getStorageLocation(deltaTable)
      assertResult(s"file:$path")(result)
    }
  }

  describe("copy a delta table to a new table") {
    it("should create a new delta table from an existing one using path") {
      val path = (os.pwd / "tmp" / "delta-copy-from-existing-path").toString()

      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Patricia", "Jackson"),
        (5, "Jose", "Travolta"),
        (6, "Gabriela", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")
      df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("lastname", "firstname")
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      val targetPath = (os.pwd / "tmp" / "delta-copy-from-existing-target-path").toString()
      DeltaHelpers.copyTable(deltaTable, targetPath = Some(targetPath))

      assertSmallDataFrameEquality(
        DeltaTable.forPath(targetPath).toDF,
        df,
        orderedComparison = false,
        ignoreNullable = true
      )
    }

    it("should copy table from existing one using table name") {
      val path = (os.pwd / "tmp" / "delta-copy-from-existing-tb-name").toString()

      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Patricia", "Jackson"),
        (5, "Jose", "Travolta"),
        (6, "Gabriela", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")
      df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("lastname")
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      val tableName  = "students"
      DeltaHelpers.copyTable(deltaTable, targetTableName = Some(tableName))
      assertSmallDataFrameEquality(
        DeltaTable.forName(spark, tableName).toDF,
        df,
        orderedComparison = false,
        ignoreNullable = true
      )
    }

    it("should fail to copy when no table name or target path is set") {
      val path = (os.pwd / "tmp" / "delta-copy-non-destination").toString()

      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Patricia", "Jackson"),
        (5, "Jose", "Travolta"),
        (6, "Gabriela", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")
      df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("lastname")
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      val exceptionMessage = intercept[JodieValidationError] {
        DeltaHelpers.copyTable(deltaTable)
      }.getMessage

      assert(exceptionMessage.contains("Either targetPath or targetTableName must be specified."))
    }

    it("should fail to copy when both table name and target path are set") {
      val path = (os.pwd / "tmp" / "delta-copy-two-destination").toString()
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Patricia", "Jackson"),
        (5, "Jose", "Travolta"),
        (6, "Gabriela", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")
      df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("lastname")
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      val tableName  = "students"
      val tablePath  = (os.pwd / "tmp" / "delta-copy-from-existing-target-path").toString()
      val exceptionMessage = intercept[JodieValidationError] {
        DeltaHelpers.copyTable(deltaTable, Some(tablePath), Some(tableName))
      }.getMessage

      assert(
        exceptionMessage.contains(
          "Ambiguous destination only one of the two must be defined targetPath or targetTableName."
        )
      )
    }
  }

  describe("Append without duplicating data") {
    it(
      "should insert data into an existing delta table and not duplicates in case some records already exists"
    ) {
      val path = (os.pwd / "tmp" / "delta-lake-inserts-no-dup").toString()
      Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta")
      )
        .toDF("id", "firstname", "lastname")
        .write
        .format("delta")
        .mode("overwrite")
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      val df = Seq(
        (4, "Maria", "Jackson"),
        (5, "Jose", "Travolta"),
        (6, "Gabriela", "Travolta"),
        (7, "Maria", "Pitt")
      )
        .toDF("id", "firstname", "lastname")

      DeltaHelpers.appendWithoutDuplicates(deltaTable, df, Seq("firstname", "lastname"))

      val expected = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Maria", "Jackson"),
        (6, "Gabriela", "Travolta"),
        (7, "Maria", "Pitt")
      ).toDF("id", "firstname", "lastname")
      val result = DeltaTable.forPath(path)
      assertSmallDataFrameEquality(
        result.toDF,
        expected,
        orderedComparison = false,
        ignoreNullable = true
      )
    }

    it("it should fail to insert data when primaryKeysColumns is empty") {
      val path = (os.pwd / "tmp" / "delta-lake-inserts-no-dup").toString()
      Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta")
      )
        .toDF("id", "firstname", "lastname")
        .write
        .format("delta")
        .mode("overwrite")
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      val df = Seq(
        (4, "Maria", "Jackson"),
        (5, "Jose", "Travolta")
      ).toDF("id", "firstname", "lastname")

      val exceptionMessage = intercept[NoSuchElementException] {
        DeltaHelpers.appendWithoutDuplicates(deltaTable, df, Seq())
      }.getMessage

      assert(exceptionMessage.contains("The attribute primaryKeysColumns must not be empty"))
    }

    it("should execute successful when an empty dataframe(appendData) is given") {
      val path = (os.pwd / "tmp" / "delta-lake-inserts-no-dup").toString()
      Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta")
      )
        .toDF("id", "firstname", "lastname")
        .write
        .format("delta")
        .mode("overwrite")
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      val df         = Seq.empty[(String, String, String)].toDF("id", "firstname", "lastname")
      DeltaHelpers.appendWithoutDuplicates(deltaTable, df, Seq("firstname", "lastname"))
      val result = DeltaTable.forPath(path)

      assertSmallDataFrameEquality(
        result.toDF,
        deltaTable.toDF,
        orderedComparison = false,
        ignoreNullable = true
      )
    }

  }
}
