package mrpowers.jodie

import org.apache.hadoop.fs.{Path => HadoopPath}
import org.scalatest.{BeforeAndAfterEach, FunSpec}

import java.net.URI

class PathSpec extends FunSpec with FileContextTestWrapper with BeforeAndAfterEach{
  override protected def afterEach(): Unit = {
    hio.remove.all(wd)
  }
  override protected def beforeEach(): Unit = {
    hio.mkdir(wd / "dir1")
    hio.write(wd / "dir1/test_data1.txt", "hola1")
  }
  describe("path creation"){

    it("should allow the creation of a path from a string"){
      val stringPath = wd.toString
      val path = hio.JodiePath(stringPath)
      assert(path.toString == stringPath)
    }

    it("should allow the creation of a path from a URI"){
      val path = hio.JodiePath(new URI(wd.toString))
      assert(path.toString == wd.toString)
    }

    it("should allow the creation of a path from a HadoopFile") {
      val path = hio.JodiePath(new HadoopPath(wd.toString))
      assert(path.toString == wd.toString)
    }

    it("should allow concatenate single string into a path"){
      val path = hio.JodiePath(wd.toString) / "github-issue"
      assert(path.toString == s"${wd.toString}/github-issue")
    }

    it("should allow concatenate multiple string into a path"){
      val path = hio.JodiePath(wd.toString) / "github-issue" / "test1/test2"
      println(path)
      assert(path.toString == s"${wd.toString}/github-issue/test1/test2")
    }

    it("should provide default directories after path creation") {
      val path = hio.JodiePath(wd.toString)
      assert(path.wd.toString.nonEmpty)
      assert(path.home.toString.nonEmpty)
      assert(path.baseName.toString.nonEmpty)
    }

    it("should provide util functions to manipulate the path"){
      val path = hio.JodiePath(wd.toString) / "data.csv"

      assert(path.ext == "csv")
      assert(path.lastName == "data.csv")
      assert(path.uri.toString == path.toString)
    }
  }

}
