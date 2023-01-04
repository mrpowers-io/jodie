package hio

import hio.Config.getFSConfiguration
import mrpowers.jodie.JodieValidationError
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission

import java.io.FileNotFoundException
import java.util

object mkdir{
  def apply(path: JodiePath, permission:FsPermission = FileContext.DIR_DEFAULT_PERM,
            configuration:Configuration = getFSConfiguration):Unit = {
    FileContext
      .getFileContext(path.uri,configuration)
      .mkdir(path.relativePath,permission,true)
  }
}

/**
 * This Operation support writing String and byte array data to disk.
 */
object write {
  def apply(path:JodiePath,fileContent:FileContent,
            createFlag: util.EnumSet[CreateFlag] = util.EnumSet.of(CreateFlag.CREATE,CreateFlag.OVERWRITE),
            configuration:Configuration = getFSConfiguration):Unit = {

    val fileContext = FileContext.getFileContext(path.uri,configuration)
    val outputStream = fileContext.create(path.relativePath, createFlag)
    val content = fileContent()
    outputStream.write(content)
    outputStream.close()
  }
}

/**
 * This operation list all the files and folder in a folder.
 * It support simple searches and wildcard searches.
 */
object ls{

  def apply(path:JodiePath,filterFunction:Path => Boolean = _ => true,
            configuration:Configuration = getFSConfiguration):Seq[String] ={
    try{
    val fileStatusIterator = FileContext
      .getFileContext(path.uri,configuration)
      .util()
      .listStatus(path.relativePath, filterFunction)

      fileStatusIterator.map( f => f.getPath.toString)
    }catch {
      case e: FileNotFoundException => throw JodieValidationError(s"The Path $path does not exists",e)
    }
  }

  def withWildCard(path: JodiePath, configuration:Configuration = getFSConfiguration): Seq[String] = {
    try {
      val fileStatusIterator = FileContext
        .getFileContext(path.uri,configuration)
        .util()
        .globStatus(path.relativePath)
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
  def apply(path: JodiePath, configuration:Configuration = getFSConfiguration):Boolean = {
    FileContext
      .getFileContext(path.uri,configuration)
      .delete(path.relativePath,false)
  }
  object all{
      def apply(path:JodiePath, configuration:Configuration = getFSConfiguration):Boolean ={
        FileContext
          .getFileContext(path.uri,configuration)
          .delete(path.relativePath,true)
      }
  }
}

object read{
  def apply(path:JodiePath, configuration:Configuration = getFSConfiguration): FSDataInputStream = {
    FileContext
      .getFileContext(path.uri,configuration)
      .open(path.relativePath)
  }

  object string{
    def apply(path: JodiePath, configuration:Configuration = getFSConfiguration): String = {
      val outputStream = FileContext
        .getFileContext(path.uri,configuration)
        .open(path.relativePath)
      val fileContent = IOUtils.toString(outputStream, hio.FileContent.CHARSET)
      outputStream.close()
      fileContent
    }
  }
}
