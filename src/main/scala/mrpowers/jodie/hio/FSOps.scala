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
    val fileStatusIterator = getFileStatus(path, filterFunction, configuration)

      fileStatusIterator.map( f => f.getPath.toString)
    }catch {
      case e: FileNotFoundException => throw JodieValidationError(s"The Path $path does not exists",e)
    }
  }

  private def getFileStatus(path: JodiePath, filterFunction: Path => Boolean = _ => true,
                            configuration: Configuration = getFSConfiguration) = {
    FileContext
      .getFileContext(path.uri, configuration)
      .util()
      .listStatus(path.relativePath, filterFunction)
  }

  private def getFileStatusWithWildCard(path: JodiePath, configuration: Configuration = getFSConfiguration) = {
    FileContext
      .getFileContext(path.uri, configuration)
      .util()
      .globStatus(path.relativePath)
  }

  def withWildCard(path: JodiePath, configuration:Configuration = getFSConfiguration): Seq[String] = {
    try {
      val fileStatusIterator = getFileStatusWithWildCard(path,configuration)
      fileStatusIterator.map( f => f.getPath.toString)
    } catch {
      case e: FileNotFoundException => throw JodieValidationError(s"The Path $path does not exists", e)
    }
  }

  object status{
    def apply(path: JodiePath, filterFunction: Path => Boolean = _ => true,
              configuration: Configuration = getFSConfiguration):Array[FileStatus] = {
      try {
        getFileStatus(path,filterFunction,configuration)
      }catch {
        case e: FileNotFoundException => throw JodieValidationError(s"The Path $path does not exists", e)
      }
    }


    def withWildCard(path: JodiePath, configuration: Configuration = getFSConfiguration): Array[FileStatus] = {
      try {
        getFileStatusWithWildCard(path,configuration)
      } catch {
        case e: FileNotFoundException => throw JodieValidationError(s"The Path $path does not exists", e)
      }
    }
  }

  //TODO: Implement a recursive list solution

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

object copy{
  //TODO: Possible improvements 1.Implement a recursive copy solution 2. Implement parallelism when copy multiple files

  def apply(originPath:JodiePath, destinationPath:JodiePath, overWrite:Boolean = false,
            configuration:Configuration = getFSConfiguration,deleteSource:Boolean = false):Unit = {
    val fileContext = FileContext
      .getFileContext(originPath.uri, configuration)

    //Validate source and destination paths are directories or files
    (originPath.ext.isEmpty, destinationPath.ext.isEmpty) match {
      // if source and destination are directories, copy all the files in the source directory
      case (true, true) =>
        val paths = ls.status(originPath)
        paths.filter(p => !p.isDirectory).foreach(srcPath => {
          val dest = destinationPath / srcPath.getPath.getName
          fileContext
            .util()
            .copy(srcPath.getPath, dest.path, deleteSource, overWrite)
        })
      // if source is file and destination is directory, copy only the file to the directory
      case (false, true) =>
        val dest = destinationPath / originPath.lastName
        fileContext
          .util()
          .copy(originPath.path, dest.path, deleteSource, overWrite)
      // if source and destination are files, copy source to destination
      case (false, false) =>
        fileContext
          .util()
          .copy(originPath.path, destinationPath.path, deleteSource, overWrite)
      // if source is directory and destination is file, produce an error
      case (true, false) =>
        throw JodieValidationError(s"can't ${if(deleteSource) "move" else "copy"} all the files of directory ${originPath.toString} into a single file ${destinationPath.toString}.")
    }
  }

  def withWildCard(originPath:JodiePath, destinationPath:JodiePath, overWrite: Boolean = false,
                   configuration: Configuration = getFSConfiguration, deleteSource:Boolean = false): Unit = {
    val paths = ls.status.withWildCard(originPath)
    val fileContext = FileContext
      .getFileContext(originPath.uri, configuration)
    (paths.isEmpty, destinationPath.ext.isEmpty) match {
      case (false,true) =>
        paths.filter(p => !p.isDirectory).foreach(srcPath => {
          val dest = destinationPath / srcPath.getPath.getName
          fileContext
            .util()
            .copy(srcPath.getPath, dest.path, deleteSource, overWrite)
        })
      case (false,false) =>
        paths.length match {
          case 1 =>
            val src = paths.head.getPath
            fileContext.util().copy(src,destinationPath.path,deleteSource,overWrite)
          case _ => throw JodieValidationError(s"can't copy all the files of ${originPath.toString} into a single file ${destinationPath.toString}.")
        }
      case (true,_) => //Empty source avoid performing copy
    }
  }


}

object move{
  def apply(originPath:JodiePath, destinationPath:JodiePath, overWrite:Boolean = false,
            configuration:Configuration = getFSConfiguration):Unit = {
    val fileContext = FileContext
      .getFileContext(originPath.uri, configuration)
    // if both source and destination have the same root, we can apply rename to avoid loading data into memory
    if (originPath.baseName.toString == destinationPath.baseName.toString){
      (originPath.ext.isEmpty, destinationPath.ext.isEmpty) match {
        case (true, true) =>
          val paths = ls.status(originPath)
          paths.filter(p => !p.isDirectory).foreach(srcPath => {
            val dest = destinationPath / srcPath.getPath.getName
            fileContext.rename(srcPath.getPath, dest.path)
          })
        case (false, true) =>
          val dest = destinationPath / originPath.lastName
          fileContext.rename(originPath.path, dest.path)
        case (false, false) =>
          fileContext.rename(originPath.path, destinationPath.path)
        case (true, false) =>
          throw JodieValidationError(s"can't move all the files of directory ${originPath.toString} into a single file ${destinationPath.toString}.")
      }
    }else{
      copy(originPath, destinationPath, overWrite, configuration, deleteSource = true)
    }
  }

  def withWildCard(originPath: JodiePath, destinationPath: JodiePath, overWrite: Boolean = false,
                   configuration: Configuration = getFSConfiguration):Unit = {

    // if both source and destination have the same root, we can apply rename to avoid loading data into memory
    if (originPath.baseName.toString == destinationPath.baseName.toString){
      val paths = ls.status.withWildCard(originPath)
      val fileContext = FileContext
        .getFileContext(originPath.uri, configuration)
      (paths.isEmpty, destinationPath.ext.isEmpty) match {
        case (false, true) =>
          paths.filter(p => !p.isDirectory).foreach(srcPath => {
            val dest = destinationPath / srcPath.getPath.getName
            fileContext
              .rename(srcPath.getPath, dest.path)
          })
        case (false, false) =>
          paths.length match {
            case 1 =>
              val src = paths.head.getPath
              fileContext.rename(src, destinationPath.path)
            case _ => throw JodieValidationError(s"can't move all the files of ${originPath.toString} into a single file ${destinationPath.toString}.")
          }
        case (true, _) =>
      }
    }else{
      copy.withWildCard(originPath,destinationPath,overWrite,configuration,deleteSource = true)
    }
  }
}


