package mrpowers.jodie

import com.github.mrpowers.spark.daria.sql.SparkSessionExt.SparkSessionMethods
import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import io.delta.tables.DeltaTable
import mrpowers.jodie.delta.DeltaConstants._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.{convertToAnyShouldWrapper, equal}

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

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

  describe("When Delta table is queried for file sizes") {
    it("should provide delta file sizes successfully") {
      val path = (os.pwd / "tmp" / "delta-table").toString()
      createBaseDeltaTable(path, rows)

      val deltaTable = DeltaTable.forPath(path)
      val actual     = DeltaHelpers.deltaFileSizes(deltaTable)

      actual("size_in_bytes") should equal(1088L)
      actual("number_of_files") should equal(1L)
      actual("average_file_size_in_bytes") should equal(1088L)
    }

    it("should not fail if the table is empty") {
      val emptyDeltaTable = DeltaTable
        .create(spark)
        .tableName("delta_empty_table")
        .addColumn("id", dataType = "INT")
        .addColumn("firstname", dataType = "STRING")
        .addColumn("lastname", dataType = "STRING")
        .execute()
      val actual = DeltaHelpers.deltaFileSizes(emptyDeltaTable)
      actual("size_in_bytes") should equal(0)
      actual("number_of_files") should equal(0)
      actual("average_file_size_in_bytes") should equal(0)
    }
  }
  describe("remove duplicate records from delta table") {
    it("should remove duplicates successful") {
      val path = (os.pwd / "tmp" / "delta-duplicate").toString()
      createBaseDeltaTable(path, rows)

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
      createBaseDeltaTable(path, rows)

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
      createBaseDeltaTable(path, rows)

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
      createBaseDeltaTable(path, rows)

      val deltaTable = DeltaTable.forPath(path)
      val primaryKey = ""
      val exceptionMessage = intercept[NoSuchElementException] {
        DeltaHelpers.removeDuplicateRecords(deltaTable, primaryKey, Seq("lastname"))
      }.getMessage

      assert(exceptionMessage.contains("the input parameter primaryKey must not be empty"))
    }

    it("should fail to remove duplicate when duplicateColumns does not exist in table") {
      val path = (os.pwd / "tmp" / "delta-duplicate-cols-no-exists").toString()
      createBaseDeltaTable(path, rows)

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
      intercept[JodieValidationError] {
        DeltaHelpers.removeDuplicateRecords(deltaTable, Seq("firstname", unknownColumn))
      }.getMessage
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
      val result     = DeltaHelpers.getStorageLocation(deltaTable)
      assertResult(s"file:$path")(result)
    }
  }

  describe("copy a delta table to a new table") {
    it("should create a new delta table from an existing one using path") {
      val path = (os.pwd / "tmp" / "delta-copy-from-existing-path").toString()

      val df = createBaseDeltaTableWithPartitions(path, Seq("lastname", "firstname"), partRows)
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

      val df: DataFrame = createBaseDeltaTableWithPartitions(path, Seq("lastname"), partRows)
      val deltaTable    = DeltaTable.forPath(path)
      val tableName     = "students"
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

      val df: DataFrame = createBaseDeltaTableWithPartitions(path, Seq("lastname"), partRows)
      val deltaTable    = DeltaTable.forPath(path)
      val exceptionMessage = intercept[JodieValidationError] {
        DeltaHelpers.copyTable(deltaTable)
      }.getMessage

      assert(exceptionMessage.contains("Either targetPath or targetTableName must be specified."))
    }

    it("should fail to copy when both table name and target path are set") {
      val path          = (os.pwd / "tmp" / "delta-copy-two-destination").toString()
      val df: DataFrame = createBaseDeltaTableWithPartitions(path, Seq("lastname"), partRows)
      val deltaTable    = DeltaTable.forPath(path)
      val tableName     = "students"
      val tablePath     = (os.pwd / "tmp" / "delta-copy-from-existing-target-path").toString()
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

  describe("Validate and append data to a delta table"){
    it("should append dataframes with optional columns"){
      val path = (os.pwd / "tmp" / "delta-lake-validate-append-valid-dataframe").toString()
      Seq(
        (1, "a", "A"),
        (2, "b", "B"),
      )
        .toDF("col1", "col2", "col3")
        .write
        .format("delta")
        .mode("overwrite")
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)

      val deltaTable = DeltaTable.forPath(path)

      val appendDf = Seq(
        (3, "c", "cat"),
        (4, "d", "dog"),
      )
        .toDF("col1", "col2", "col4")

      DeltaHelpers.validateAppend(deltaTable, appendDf, List("col1", "col2"), List("col4"))

      val expected = Seq(
        (1, "a", "A", null),
        (2, "b", "B", null),
        (3, "c", null, "cat"),
        (4, "d", null, "dog"),
      )
        .toDF("col1", "col2", "col3", "col4")

      val result = DeltaTable.forPath(path)

      assertSmallDataFrameEquality(
        result.toDF,
        expected,
        orderedComparison = false,
        ignoreNullable = true
      )

    }
    it("should fail to append dataframes with columns that are not on the accept list"){
      val path = (os.pwd / "tmp" / "delta-lake-validate-cols-in-accepted-list").toString()
      Seq(
        (1, "a", "A"),
        (2, "b", "B"),
      )
        .toDF("col1", "col2", "col3")
        .write
        .format("delta")
        .mode("overwrite")
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)

      val deltaTable = DeltaTable.forPath(path)

      val appendDf = Seq(
        (4, "b", "A"),
        (5, "y", "C"),
        (6, "z", "D"),
      )
        .toDF("col1", "col2", "col5")


      val exceptionMessage = intercept[IllegalArgumentException] {
        DeltaHelpers.validateAppend(deltaTable, appendDf, List("col1", "col2"),
          List("col4") )
      }.getMessage

      assert(exceptionMessage.contains("The following columns are not part of the current Delta table. If you want to add these columns to the table, you must set the optionalCols parameter: List(col5)"))
    }
    it("should fail to append dataframes with missing required columns"){
      val path = (os.pwd / "tmp" / "delta-lake-validate-missing-required-cols").toString()
      Seq(
        (1, "a", "A"),
        (2, "b", "B"),
      )
        .toDF("col1", "col2", "col3")
        .write
        .format("delta")
        .mode("overwrite")
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)

      val deltaTable = DeltaTable.forPath(path)

      val appendDf = Seq(
        (4, "A"),
        (5, "C"),
        (6, "D"),
      )
        .toDF("col1", "col4")


      val exceptionMessage = intercept[IllegalArgumentException] {
        DeltaHelpers.validateAppend(deltaTable, appendDf, List("col1", "col2"),
          List("col4"))
      }.getMessage

      assert(exceptionMessage.contains("The base Delta table has these columns List(col1, col4), but these columns are required List(col1, col2)"))

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
        (8, "Gabriela", "Travolta"),
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

    it("it should fail to insert data when compositeKey is empty") {
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

      assert(exceptionMessage.contains("The attribute compositeKey must not be empty"))
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

  describe("find composite key in a table") {
    it("should not find the composite key in the table") {
      val path = (os.pwd / "tmp" / "delta-tbl").toString()
      Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson")
      )
        .toDF("id", "firstname", "lastname")
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)

      val deltaTable = DeltaTable.forPath(path)
      val result     = DeltaHelpers.findCompositeKeyCandidate(deltaTable, Seq("id"))

      assertResult(Nil)(result)
    }

    it("should find the composite key in the table") {
      val path = (os.pwd / "tmp" / "delta-tbl").toString()
      Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Willis")
      )
        .toDF("id", "firstname", "lastname")
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      val result     = DeltaHelpers.findCompositeKeyCandidate(deltaTable, Seq("id"))
      val expected   = Seq("firstname", "lastname")
      assertResult(expected)(result)
    }

    it("should find the composite key in the table when cols are excluded") {
      val path = (os.pwd / "tmp" / "delta-tbl").toString()
      Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Willis")
      )
        .toDF("id", "firstname", "lastname")
        .write
        .format("delta")
        .mode("overwrite")
        .option("delta.logRetentionDuration", "interval 30 days")
        .save(path)
      val deltaTable = DeltaTable.forPath(path)
      val result     = DeltaHelpers.findCompositeKeyCandidate(deltaTable)
      val expected   = Seq("id")
      assertResult(expected)(result)
    }
  }

  describe("Generate MD5 from columns") {
    it("should generate a new md5 column from different columns of a dataframe") {
      val df = Seq(
        (1, "Benito", "Jackson"),
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta")
      )
        .toDF("id", "firstname", "lastname")

      val resultDF = DeltaHelpers.withMD5Columns(df, List("firstname", "lastname"), "unique_column")
      val expectedDF = Seq(
        (1, "Benito", "Jackson", "3456d6842080e8188b35f515254fece8"),
        (2, "Maria", "Willis", "4fd906b56cc15ca517c554b215597ea1"),
        (3, "Jose", "Travolta", "3b3814001b13695931b6df8670172f91")
      ).toDF("id", "firstname", "lastname", "unique_column")

      assertSmallDataFrameEquality(
        actualDF = resultDF,
        expectedDF = expectedDF,
        ignoreNullable = true,
        orderedComparison = false
      )
    }

    it("should generate a new md5 from different columns of a delta table") {
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
      val resultDF =
        DeltaHelpers.withMD5Columns(deltaTable, List("id", "firstname", "lastname"), "unique_id")

      val expectedDF = Seq(
        (1, "Benito", "Jackson", "cad17f15341ed95539e098444a4c8050"),
        (2, "Maria", "Willis", "3e1e9709234c6250c74241d5886d5073"),
        (3, "Jose", "Travolta", "1f1ac7f74f43eff911a92f7e28069271")
      ).toDF("id", "firstname", "lastname", "unique_id")

      assertSmallDataFrameEquality(
        actualDF = resultDF,
        expectedDF = expectedDF,
        ignoreNullable = true,
        orderedComparison = false
      )
    }
  }

  describe("Generate metrics for optimize functions on Delta Table") {
    it("should return valid file sizes and num records for non partitioned tables") {
      val path = (os.pwd / "tmp" / "delta-table-non-partitioned").toString()
      createBaseDeltaTable(path, rows)
      val fileSizeDF   = DeltaHelpers.deltaFileSizeDistribution(path)
      val numRecordsDF = DeltaHelpers.deltaNumRecordDistribution(path)
      fileSizeDF.count() should equal(1L)
      assertDistributionCount(
        fileSizeDF,
        (0, 1L, 1088.0, null, 1088L, 1088L, Array(1088, 1088, 1088, 1088, 1088, 1088))
      )
      numRecordsDF.count() should equal(1L)
      assertDistributionCount(numRecordsDF, (0, 1L, 7.0, null, 7L, 7L, Array(7, 7, 7, 7, 7, 7)))
    }
    it("should return valid file sizes and num records for single partitioned tables") {
      val path = (os.pwd / "tmp" / "delta-table-single-partition").toString()
      createBaseDeltaTableWithPartitions(path, Seq("lastname"), partRows)
      val fileSizeDF   = DeltaHelpers.deltaFileSizeDistribution(path, Some("lastname='Travolta'"))
      val numRecordsDF = DeltaHelpers.deltaNumRecordDistribution(path, Some("lastname='Travolta'"))
      fileSizeDF.count() should equal(1L)
      assertDistributionCount(
        fileSizeDF,
        (1, 1L, 756.0, null, 756, 756, Array(756, 756, 756, 756, 756, 756))
      )
      numRecordsDF.count() should equal(1L)
      assertDistributionCount(numRecordsDF, (1, 1L, 3.0, null, 3, 3, Array(3, 3, 3, 3, 3, 3)))
    }
    it("should return valid file sizes and num records for multiple partitioned tables") {
      val path = (os.pwd / "tmp" / "delta-table-multi-partition").toString()
      createBaseDeltaTableWithPartitions(path, Seq("lastname", "firstname"), partRows)
      val fileSizeDF = DeltaHelpers.deltaFileSizeDistribution(
        path,
        Some("lastname='Travolta' and firstname='Jose'")
      )
      val numRecordsDF = DeltaHelpers.deltaNumRecordDistribution(
        path,
        Some("lastname='Travolta' and firstname='Jose'")
      )
      fileSizeDF.count() should equal(1L)
      assertDistributionCount(
        fileSizeDF,
        (2, 1L, 456.0, null, 456, 456, Array(456, 456, 456, 456, 456, 456))
      )
      numRecordsDF.count() should equal(1L)
      assertDistributionCount(numRecordsDF, (2, 1L, 2.0, null, 2, 2, Array(2, 2, 2, 2, 2, 2)))
    }

    it("should return valid file sizes in megabytes") {
      val path = (os.pwd / "tmp" / "delta-table-multi-files").toString()

      def getDF(partition: String) = {
        (1 to 10000)
          .toDF("id")
          .collect()
          .map(_.getInt(0))
          .map(id => (id, partition, id + 10))
          .toSeq
      }

      (getDF("dog") ++ getDF("cat") ++ getDF("bird"))
        .toDF("id", "animal", "age")
        .write
        .mode("overwrite")
        .format("delta")
        .partitionBy("animal")
        .save(path)
      val fileSizeDF = DeltaHelpers.deltaFileSizeDistributionInMB(path)
      val size       = 0.07698249816894531
      fileSizeDF.count() should equal(3)
      assertDistributionCount(
        fileSizeDF,
        (1, 1L, size, null, size, size, Array(size, size, size, size, size, size))
      )
    }

    describe("Generate file overlap metrics on running filter queries") {
      it("should return valid metrics for partitioned tables") {
        val path = (os.pwd / "tmp" / "delta-table-min-max-part").toString()
        spark.conf.set("spark.sql.files.maxRecordsPerFile", "4")
        createBaseDeltaTableWithPartitions(path, Seq("lastname"), minMaxRows)

        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(path, "lastname = 'Travolta'"),
          (3, 7, 3, 7, 7, 7, List())
        )
        // Min Max Query
        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(
            path,
            "lastname = 'Travolta' " +
              "and id >= 10 and id <= 12"
          ),
          (2, 5, 3, 7, 7, 7, List())
        )
        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(path, "id <= 10 "),
          (4, 4, 7, 7, 7, 7, List())
        )
        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(path, "id >= 12"),
          (5, 5, 7, 7, 7, 7, List())
        )
        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(
            path,
            "snapshot.id = update.id and " +
              "lastname = 'Travolta' and id >= 10 and id <= 12 and firstname like '%Joh%'"
          ),
          (2, 5, 3, 7, 7, 7, List("snapshot.id", "update.id"))
        )
        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(path, "lastname = 'Travolta' and id <= 10 and id >= 12"),
          (0, 2, 3, 7, 7, 7, List())
        )
      }
      it("should return valid metrics for non partitioned tables") {
        val path = (os.pwd / "tmp" / "delta-table-min-max-no-part").toString()
        spark.conf.set("spark.sql.files.maxRecordsPerFile", "4")
        createBaseDeltaTable(path, minMaxRows)

        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(path, "id <= 10 "),
          (3, 3, 6, 6, 6, 6, List())
        )
        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(path, "id >= 12"),
          (4, 4, 6, 6, 6, 6, List())
        )
        // Min Max Query
        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(path, "id >= 10 and id <= 12"),
          (1, 1, 6, 6, 6, 6, List())
        )
        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(
            path,
            "id >= 10 and id <= 12 " +
              "and firstname like '%Joh%'"
          ),
          (1, 1, 6, 6, 6, 6, List())
        )
        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(
            path,
            "lastname = 'Travolta' " +
              "and id <= 10 and id >= 12"
          ),
          (1, 1, 6, 6, 6, 6, List())
        )
        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(
            path,
            "id >= 10 and id <= 12 " +
              "and (firstname = 'John' or firstname = 'Maria')"
          ),
          (1, 1, 6, 6, 6, 6, List())
        )
      }
      it("should return valid metrics even when unresolved column names are present in the query") {
        val path = (os.pwd / "tmp" / "delta-table-min-max-with-part").toString()
        spark.conf.set("spark.sql.files.maxRecordsPerFile", "4")
        createBaseDeltaTableWithPartitions(path, Seq("lastname"), minMaxRows)

        assertNumFilesCount(
          DeltaHelpers.getNumShuffleFiles(
            path,
            "snapshot.id = update.id and " +
              "lastname = 'Travolta' and id >= 10 and id <= 12"
          ),
          (2, 5, 3, 7, 7, 7, List("snapshot.id", "update.id"))
        )
      }
      spark.conf.set("spark.sql.files.maxRecordsPerFile", 0)
    }
    it("when rows are not in any order"){
      val path = (os.pwd / "tmp" / "delta-table-min-max-without-order").toString()
      spark.conf.set("spark.sql.files.maxRecordsPerFile", "4")
      createBaseDeltaTable(path, noOrderRows)

      val actualBefore = DeltaHelpers.getNumShuffleFiles(
        path,
        "snapshot.id = update.id and " +
          " id >= 10 and id <= 12 "
      )
      DeltaTable.forPath(spark, path).optimize().executeZOrderBy("id")
      val actualAfter = DeltaHelpers.getNumShuffleFiles(
        path,
        "snapshot.id = update.id and " +
          " id >= 10 and id <= 12 "
      )
    }
  }

  val rows = Seq(
    (1, "Benito", "Jackson"),
    (2, "Maria", "Willis"),
    (3, "Jose", "Travolta"),
    (4, "Benito", "Jackson"),
    (5, "Jose", "Travolta"),
    (6, "Jose", "Travolta"),
    (7, "Maria", "Pitt")
  )

  val partRows = Seq(
    (1, "Benito", "Jackson"),
    (2, "Maria", "Willis"),
    (3, "Jose", "Travolta"),
    (4, "Patricia", "Jackson"),
    (5, "Jose", "Travolta"),
    (6, "Gabriela", "Travolta"),
    (7, "Maria", "Pitt")
  )
  val minMaxRows: Seq[(Int, String, String)] = rows ++ Seq(
    (8, "Benito", "Jackson"),
    (9, "Maria", "Willis"),
    (10, "Jose", "Travolta"),
    (11, "Benito", "Jackson"),
    (12, "Jose", "Travolta"),
    (13, "Jose", "Travolta"),
    (14, "Maria", "Pitt"),
    (15, "Jose", "Travolta"),
    (16, "Jose", "Travolta"),
    (17, "Maria", "Pitt"),
    (18, "Benito", "Jackson"),
    (19, "Maria", "Willis"),
    (20, "Jose", "Travolta"),
    (21, "Benito", "Jackson"),
    (22, "Jose", "Travolta"),
    (23, "Jose", "Travolta"),
    (24, "Maria", "Pitt")
  )

  val noOrderRows = Seq(
    (11, "Benito", "Jackson"),
    (1, "Benito", "Jackson"),
    (21, "Benito", "Jackson"),
    (3, "Jose", "Travolta"),
    (18, "Benito", "Jackson"),
    (14, "Maria", "Pitt"),
    (5, "Jose", "Travolta"),
    (23, "Jose", "Travolta"),
    (7, "Maria", "Pitt"),
    (8, "Benito", "Jackson"),
    (9, "Maria", "Willis"),
    (10, "Jose", "Travolta"),
    (17, "Maria", "Pitt"),
    (12, "Jose", "Travolta"),
    (16, "Jose", "Travolta"),
    (4, "Patricia", "Jackson"),
    (6, "Gabriela", "Travolta"),
    (19, "Maria", "Willis"),
    (20, "Jose", "Travolta"),
    (15, "Jose", "Travolta"),
    (22, "Jose", "Travolta"),
    (13, "Jose", "Travolta"),
    (24, "Maria", "Pitt"),
    (2, "Maria", "Willis")
  )

  private def createBaseDeltaTable(path: String, data: Seq[(Int, String, String)]): Unit = {
    data.toDF("id", "firstname", "lastname").write.format("delta").mode("overwrite").save(path)
  }

  private def createBaseDeltaTableWithPartitions(
      path: String,
      partitionBy: Seq[String],
      data: Seq[(Int, String, String)]
  ) = {
    val df = data.toDF("id", "firstname", "lastname")
    df.write
      .format("delta")
      .mode("overwrite")
      .partitionBy(partitionBy: _*)
      .option("delta.logRetentionDuration", "interval 30 days")
      .save(path)
    df
  }

  private def assertDistributionCount(
      df: DataFrame,
      expected: (Int, Long, Double, Any, Any, Any, Array[Double])
  ) = {
    val actual = df.take(1)(0)
    actual.getAs[mutable.WrappedArray[(String, String)]](0).length should equal(expected._1)
    actual.getAs[Long](1) should equal(expected._2)
    actual.getAs[Double](2) should equal(expected._3)
    actual.getAs[Double](3) should equal(expected._4)
    actual.getAs[Long](4) should equal(expected._5)
    actual.getAs[Long](5) should equal(expected._6)
    actual.getAs[Array[Double]](6) should equal(expected._7)
  }

  private def assertNumFilesCount(
      actual: Map[String, Any],
      expected: Tuple7[Int, Int, Int, Int, Int, Long, Seq[String]]
  ) = {
    actual.map { a =>
      if (a._1.contains(OVERALL)) {
        a._2 should equal(expected._1)
      } else if (a._1.contains(MIN_MAX)) {
        a._2 should equal(expected._2)
      } else if (a._1.contains(EQUALS)) {
        a._2 should equal(expected._3)
      } else if (a._1.contains(LEFT_OVER)) {
        a._2 should equal(expected._4)
      } else if (a._1.contains(UNRESOLVED)) {
        a._2 should equal(expected._5)
      } else if (a._1.contains(TOTAL_NUM_FILES)) {
        a._2 should equal(expected._6)
      } else if (a._1.contains(UNRESOLVED_COLS)) {
        a._2 should equal(expected._7)
      } else {
        fail("Unexpected key")
      }
    }
  }


  describe("Validate whether a set of columns can serve as a composite key candidate") {
    it("Should fail when the list of columns to validate for the composite key is empty") {
      val path = (os.pwd / "tmp" / "delta-tbl").toString()
      Seq((1, "Benito", "Jackson"))
        .toDF("id", "firstname", "lastname")
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)

      val expected = "At least one column must be specified."


      val deltaTable = DeltaTable.forPath(path)
      Try(DeltaHelpers.isCompositeKeyCandidate(deltaTable, List.empty)) match {
        case Failure(exception) => assertResult(expected)(exception.getMessage)
        case Success(_) => fail("isCompositeKeyCandidate function should return an exception when the list of columns is empty")
      }
    }

    it("Should fail when the list of columns to validate for the composite key doesn't exit on the DeltaTable") {
      val path = (os.pwd / "tmp" / "delta-tbl").toString()
      val df = Seq((1, "Benito", "Jackson")).toDF("id", "firstname", "lastname")
      val cols = List("firstname", "nickName")

      df
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)

      val expected = s"The base table has these columns ${df.columns.mkString(",")}, but these columns are required ${cols.mkString(",")}"
      val deltaTable = DeltaTable.forPath(path)
      Try(DeltaHelpers.isCompositeKeyCandidate(deltaTable, cols)) match {
        case Failure(exception) => assertResult(expected)(exception.getMessage)
        case Success(_) => fail("isCompositeKeyCandidate function should return an exception when the list of columns is empty")
      }
    }

    it("Should return true when the compose key is a valid key") {
      val path = (os.pwd / "tmp" / "delta-tbl").toString()
      val df = Seq(
        (2, "Maria", "Willis"),
        (3, "Jose", "Travolta"),
        (4, "Benito", "Jackson")
      ).toDF("id", "firstname", "lastname")

      val cols = List("id", "firstname")

      df
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)

      val deltaTable = DeltaTable.forPath(path)

      assert(DeltaHelpers.isCompositeKeyCandidate(deltaTable, cols))
    }


    it("Should return false when the compose key is an invalid key") {
      val path = (os.pwd / "tmp" / "delta-tbl").toString()
      val df = Seq(
        (2, "Maria", "Willis"),
        (3, "Benito", "Jackson"),
        (4, "Benito", "Jackson")
      ).toDF("id", "firstname", "lastname")

      val cols = List("firstname", "lastname")

      df
        .write
        .format("delta")
        .mode("overwrite")
        .save(path)

      val deltaTable = DeltaTable.forPath(path)

      assert(!DeltaHelpers.isCompositeKeyCandidate(deltaTable, cols))
    }
  }
}
