package hio

import mrpowers.jodie.JodieValidationError
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs._

import java.io.FileNotFoundException
import java.util

object mkdir{
  def apply(path: JodiePath):Unit = {
    FileContext
      .getFileContext(path.uri)
      .mkdir(path.path,FileContext.DIR_DEFAULT_PERM,true)
  }

  def apply(path:JodiePath,permission:FsPermission):Unit = {
    FileContext
      .getFileContext(path.uri)
      .mkdir(path.path, permission, true)
  }
}

object write {
  def apply(path:JodiePath,fileContent:FileContent,createFlag: CreateFlag = CreateFlag.CREATE):Unit = {
    val fileContext = FileContext.getFileContext(path.uri)
    val outputStream = fileContext.create(path.path, util.EnumSet.of(createFlag))
    val content = fileContent()
    outputStream.write(content,0,content.length)
    outputStream.close()
  }
}

/**
 * This operation list all the files and folder in a folder. It provides two function...
 */
object ls{

  def apply(path:JodiePath,filterFunction:Path => Boolean = _ => true):Seq[String] ={
    try{
    val fileStatusIterator = FileContext
      .getFileContext(path.uri)
      .util()
      .listStatus(path.path, filterFunction)

      fileStatusIterator.map( f => f.getPath.toString)
    }catch {
      case e: FileNotFoundException => throw JodieValidationError(s"The Path $path does not exists",e)
    }
  }

  def withWildCard(path: JodiePath): Seq[String] = {
    try {
      val fileStatusIterator = FileContext
        .getFileContext(path.uri)
        .util()
        .globStatus(path.path)
      fileStatusIterator.map( f => f.getPath.toString)
    } catch {
      case e: FileNotFoundException => throw JodieValidationError(s"The Path $path does not exists", e)
    }
  }

  implicit class convertFunction(f:Path => Boolean) extends PathFilter{
    override def accept(path: Path): Boolean = f(path)
  }

}

object remove{
  def apply(path: JodiePath):Boolean = {
    FileContext
      .getFileContext(path.uri)
      .delete(path.path,false)
  }
  object all{
      def apply(path:JodiePath):Boolean ={
        FileContext
          .getFileContext(path.uri)
          .delete(path.path,true)
      }
  }
}
