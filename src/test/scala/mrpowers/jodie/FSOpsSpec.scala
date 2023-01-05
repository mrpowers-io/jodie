package mrpowers.jodie

import org.apache.commons.io.IOUtils
import org.scalatest.{BeforeAndAfterEach, FunSpec}

import java.io.{ByteArrayOutputStream, IOException, ObjectOutputStream}

class FSOpsSpec extends FunSpec with FileContextTestWrapper with BeforeAndAfterEach {
  override protected def afterEach(): Unit = {
    hio.remove.all(wd)
  }

  override protected def beforeEach(): Unit = {
    hio.mkdir(wd / "dir1")
    hio.mkdir(wd / "dir2")
    hio.mkdir(wd / "dir3")
    hio.mkdir(wd / "dir1" / "dir4")
    hio.write(wd / "dir1/test_data1.txt", "hola1")
    hio.write(wd / "dir1/test_data2.txt", "hola2")
    hio.write(wd / "dir1/test_data3.txt", "hola3")
  }

  describe("list files/folders operations"){

    it("should list all the existing folders in a path"){
      val result = hio.ls(wd)
      val expected = List((wd/"dir1").toString,(wd/"dir2").toString,(wd/"dir3").toString)
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

    it("should remove all txt files from a folder using wildcard"){
      //todo: could be implemented using hio.ls.wildcard + hio.remove but is going to be slow
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
}
