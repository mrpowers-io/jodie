package mrpowers.jodie

import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterEach, FunSpec}

import java.io.{ByteArrayOutputStream, IOException, ObjectOutputStream}
import scala.collection.mutable

class FSOpsSpec extends FunSpec with FileContextTestWrapper with BeforeAndAfterEach {
  override protected def afterEach(): Unit = {
    hio.remove.all(wd)
  }

  override protected def beforeEach(): Unit = {
    hio.mkdir(wd / "dir1")
    hio.mkdir(wd / "dir2")
    hio.mkdir(wd / "dir3")
    hio.mkdir(wd / "dir1" / "dir4" / "dir5")
    hio.mkdir(wd / "dir6")
    hio.write(wd / "dir1/test_data1.txt", "hola1")
    hio.write(wd / "dir1/test_data2.txt", "hola2")
    hio.write(wd / "dir1/test_data3.txt", "hola3")
    hio.write(wd / "dir6/test_data6.txt", "hola6")
  }

  describe("list files/folders operations"){

    it("should list all the existing folders in a path"){
      val result = hio.ls(wd)
      val expected = List((wd/"dir1").toString,(wd/"dir2").toString,(wd/"dir3").toString,(wd/"dir6").toString)
      assert(result.sorted==expected)
    }

    it("should return an error when the folder does not exists"){
      val path = wd / "not_existing_folder"
      val messageError = intercept[JodieValidationError]{
        hio.ls(path)
      }.getMessage
      val expectedMessage = s"The Path $path does not exists"
      assert(messageError == expectedMessage)
    }

    it("should list all the files given the wildcard"){
      val result = hio.ls.withWildCard(wd /"dir1" /"*.txt")
      val expected = List(
        (wd / "dir1/test_data1.txt").toString,
        (wd / "dir1/test_data2.txt").toString,
        (wd / "dir1/test_data3.txt").toString)
      assert(result == expected)
    }
  }

  describe("create folder operations"){
    it("should create a single folder"){
      hio.mkdir(wd / "mkoperation")
      val pathNames = hio.ls(wd)
      val expected = (wd/"mkoperation").toString
      assert(pathNames.contains(expected))
    }

    it("should recursively create folders"){
      hio.mkdir(wd / "dir1" / "dir4" / "dir5")

      assert(hio.ls(wd / "dir1").contains((wd / "dir1" / "dir4").toString))
      assert(hio.ls(wd / "dir1" / "dir4").contains((wd / "dir1"  / "dir4" / "dir5").toString))
    }
  }


  describe("write operation"){
   it("should write a file with string data to disk"){
     val content =
       """
         |name,lastname,age
         |Maria,Willis,36
         |Benito,Jackson,28
         |""".stripMargin

     val testFile = wd / "dir1" / "test_data4.csv"
     hio.write(testFile, content)
     val result = hio.ls(testFile)
     assert(result.contains(testFile.toString))
   }

    it("should overwrite an existing file with string data to disk") {
      val content =
        """
          |User data
          |of multiple
          |lines
          |""".stripMargin

      val testFile = wd / "dir1" / "test_data3.txt"
      hio.write(testFile, content)
      val result = hio.read.string(testFile)
      assert(result == content)
    }

    it("should write objects "){
      class User(val name: String, val email: String) extends Serializable
      val user = new User("bj", "bj@gmail.com")

      val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(stream)
      oos.writeObject(user)
      oos.close()

      val filePath = wd/"dir3"/ "user.txt"
      hio.write(filePath,stream.toByteArray)
      val result = hio.ls(filePath)
      assert(result.contains(filePath.toString))
    }
  }

  describe("delete folder and files operation"){
    it("should remove one file"){
      val filePath = wd / "dir1/test_data1.txt"
      hio.ls(filePath)
      hio.remove(filePath)
      val messageException = intercept[JodieValidationError]{
        hio.ls(filePath)
      }.getMessage
      assert(messageException == s"The Path $filePath does not exists")
    }

    it("should remove one folder") {
      val filePath = wd / "dir2"
      hio.remove(filePath)
      val result = hio.ls(wd)
      assert(!result.contains(filePath.toString))
    }

    it("should remove all nested folder/files"){
      val filePath = wd / "dir1"
      hio.remove.all(filePath)
      val result = hio.ls(wd)
      assert(!result.contains(filePath.toString))
    }

    it("should fail to remove a folder that have sub folders") {
      val filePath = wd / "dir1"
      val errorMessage = intercept[IOException]{
        hio.remove(filePath)
      }.getMessage

      assert(errorMessage.contains(s"Directory ${filePath.relativePath.toString} is not empty"))
    }
  }

  describe("Read the content of a file into memory"){
    it("should load the content of a file into memory"){
      val filePath = wd / "dir1/test_data3.txt"
      val outputStream = hio.read(filePath)
      val result = IOUtils.toString(outputStream, hio.FileContent.CHARSET)
      outputStream.close()
      assert(result == "hola3")
    }

    it("should load the content of a file into memory and parse it to string ") {
      val filePath = wd / "dir1/test_data2.txt"
      val result = hio.read.string(filePath)
      assert(result == "hola2")
    }
  }

  describe("copy files"){

    it("should copy file to an existing empty folder"){
      val src = wd / "dir1/test_data1.txt"
      val dest = wd / "dir3/"
      hio.copy(src,dest)
      val result = hio.ls(dest )
      assertResult(Seq((dest / "test_data1.txt").toString))(result)
    }

    it("should copy a file when the file already exist in the destination"){
      val src = wd / "dir1/test_data1.txt"
      val dest = wd / "dir3/test_data1.txt"
      hio.write(dest, "abcd")
      hio.copy(src, dest,overWrite = true)
      val result = hio.ls(dest)
      assertResult(Seq(dest.toString))(result)
    }

    it("should fail to copy a file when the file already exist in the destination"){
      val src = wd / "dir1/test_data1.txt"
      val dest = wd / "dir3/test_data1.txt"
      hio.write(dest, "abcd")
      val errorMessage = intercept[IOException]{
        hio.copy(src, dest)
      }.getMessage
      assertResult(s"Target ${dest.path.toString}/${src.lastName} already exists")(errorMessage)
    }

    it("should copy all the files from an origin folder into a destination"){
      val src = wd / "dir1/*.txt"
      val dest = wd / "dir3/"
      hio.copy.withWildCard(src,dest)
      val result = hio.ls(dest)
      val expectedResult = mutable.ArraySeq((dest / "test_data1.txt").toString, (dest / "test_data3.txt").toString, (dest / "test_data2.txt").toString)
      assertResult(expectedResult.sorted)(result.sorted)
    }

    it("should fail to copy a folder content into a file"){
      val src = wd / "dir1/"
      val dest = wd / "dir3/test_data.txt"
      val errorMessage = intercept[JodieValidationError]{
        hio.copy(src,dest)
      }.getMessage
      assertResult(s"can't copy all the files of directory ${src.toString} into a single file ${dest.toString}.")(errorMessage)
    }

    it("should fail to copy multiples files using wildcard into a file") {
      val src = wd / "dir1/*.txt"
      val dest = wd / "dir3/test_data.txt"
      val errorMessage = intercept[JodieValidationError]{
        hio.copy.withWildCard(src,dest)
      }.getMessage
      assertResult(s"can't copy all the files of ${src.toString} into a single file ${dest.toString}.")(errorMessage)
    }

    it("should copy a file into a target file when using wildcard and the search result return one file") {
      val src = wd / "dir6/*.txt"
      val dest = wd / "dir3/test_data.txt"
      hio.copy.withWildCard(src,dest)
      val resultPath = hio.ls(dest)
      assertResult(List(dest.toString))(resultPath)
    }

    it("should copy from a local filesystem to a s3 object store"){
     /* val src = wd / "dir1/test_data2.txt"
      val dest = hio.JodiePath("s3a://hdfs-fs-test/")
      hio.copy(src, dest,overWrite = true)
      val result = hio.ls(dest / "test_data2.txt")
      val expected = Seq((dest / "test_data2.txt").toString)
      assertResult(expected)(result)*/
      //TODO: It works with my personal aws credentials but it should be an integration test
    }

    it("should copy from a local filesystem to a s3 object store using wildcard") {

     // val src = wd / "dir1/*.txt"
     // val dest = hio.JodiePath("s3a://hdfs-fs-test/")
     // hio.copy.withWildCard(src, dest,overWrite = true)
     // val result = hio.ls.withWildCard(dest / "*.txt")
     // val expected = Seq((dest/"test_data1.txt").toString,(dest/"test_data2.txt").toString,(dest/"test_data3.txt").toString)
     // assertResult(expected)(result)
     // It works with my personal aws credentials but this should converted into an integration test
    }
  }

  describe("move files"){
    it("should move file successfully when source is a file and destination is a folder"){
      val src = wd / "dir1/test_data1.txt"
      val dest = wd / "dir3/"
      hio.move(src,dest)
      val destResult = hio.ls(dest)
      val errorMessage = intercept[JodieValidationError]{
        hio.ls(src)
      }.getMessage
      assertResult(Seq((dest / "test_data1.txt").toString))(destResult)
      assertResult(s"The Path ${src.toString} does not exists")(errorMessage)
    }

    it("should move file successfully when source is a file and destination is a file"){
      val src = wd / "dir1/test_data1.txt"
      val dest = wd / "dir3/test_data1.txt"
      hio.move(src,dest)
      val destResult = hio.ls(dest)
      val errorMessage = intercept[JodieValidationError] {
        hio.ls(src)
      }.getMessage
      assertResult(Seq(dest.toString))(destResult)
      assertResult(s"The Path ${src.toString} does not exists")(errorMessage)
    }

    it("should move files successfully when source is a folder and destination is a folder") {
      val src = wd / "dir1/"
      val dest = wd / "dir3/"
      hio.move(src,dest)
      val destResult = hio.ls(dest)
      val srcResult = hio.ls.status(src)
      val destExpected = Seq((dest / "test_data1.txt").toString,(dest / "test_data3.txt").toString,(dest / "test_data2.txt").toString)
      assertResult(destExpected.sorted)(destResult.sorted)
      assert(srcResult.forall(f => f.isDirectory))
    }

    it("should fail to move files when source is a folder and destination is a file") {
      val src = wd / "dir1/"
      val dest = wd / "dir3/data_file.txt"
      val errorMessage = intercept[JodieValidationError]{
        hio.move(src,dest)
      }.getMessage
      assertResult(s"can't move all the files of directory ${src.toString} into a single file ${dest.toString}.")(errorMessage)
    }

    it("should fail to move files when using wildcard and source are multiple files and destination is a file") {
      val src = wd / "dir1/*.txt"
      val dest = wd / "dir3/data_file.txt"
      val errorMessage = intercept[JodieValidationError] {
        hio.move.withWildCard(src, dest)
      }.getMessage
      assertResult(s"can't move all the files of ${src.toString} into a single file ${dest.toString}.")(errorMessage)
    }

    it("should move files successfully when using wildcard and source is a file and destination is a file") {
      val src = wd / "dir6/*.txt"
      val dest = wd / "dir3/data_file.txt"
      hio.move.withWildCard(src,dest)
      val srcResult = hio.ls.withWildCard(src)
      val destResult = hio.ls(dest)
      assert(srcResult.isEmpty)
      assert(destResult.nonEmpty)
    }

    it("should move from a local filesystem to a s3 object store using wildcard") {
      //val src = wd / "dir1/*.txt"
      //val dest = hio.JodiePath("s3a://hdfs-fs-test/")
      //hio.move.withWildCard(src, dest, overWrite = true)
      //val srcResult = hio.ls.withWildCard(src)
      //val destResult = hio.ls.withWildCard(dest / "*.txt")
      //val expected = Seq((dest/"test_data1.txt").toString,(dest/"test_data2.txt").toString,(dest/"test_data3.txt").toString)
      //assertResult(expected)(destResult)
      //assert(srcResult.isEmpty)
      // It works with my personal aws credentials but it should be an integration test
    }


    it("should move files successfully when using wildcard and source are multiple files and destination is a folder") {
      val src = wd / "dir1/*.txt"
      val dest = wd / "dir3"
      hio.move.withWildCard(src,dest)
      val srcResult = hio.ls.withWildCard(src)
      val destResult = hio.ls(dest)
      val expected = Seq((dest/"test_data1.txt").toString,(dest/"test_data3.txt").toString,(dest/"test_data2.txt").toString)
      assert(srcResult.isEmpty)
      assertResult(expected.sorted)(destResult.sorted)
    }

  }
}
