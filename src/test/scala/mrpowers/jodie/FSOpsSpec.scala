package mrpowers.jodie

import org.scalatest.{BeforeAndAfterEach, FunSpec}

class FSOpsSpec extends FunSpec with FileContextTestWrapper with BeforeAndAfterEach {
  override protected def afterEach(): Unit = {
    hio.remove.all(wd)
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
        "file:/Users/brayan_jules/projects/open-source/jodie/hio-tmp/dir1/test_data1.txt",
        "file:/Users/brayan_jules/projects/open-source/jodie/hio-tmp/dir1/test_data2.txt",
        "file:/Users/brayan_jules/projects/open-source/jodie/hio-tmp/dir1/test_data3.txt")
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
         |User data
         |of multiple
         |lines
         |""".stripMargin

     hio.write(wd / "dir1" / "dir4" / "jodie2.txt", content)

     val jodieFile = wd / "dir1" / "dir4" / "jodie2.txt"
     val result = hio.ls(jodieFile)

     assert(result.contains(jodieFile.toString))

   }

    it("should write objects "){
      case class User(name: String, email: String)
      val user = User("bj", "bj@gmail.com")
    }
  }

  describe("delete folder and files operation"){

  }
}
