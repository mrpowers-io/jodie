package mrpowers.jodie

import hio.JodiePath
import org.apache.hadoop.fs.FileContext

trait FileContextTestWrapper{

  lazy val fileContext: FileContext = FileContext.getLocalFSFileContext

  lazy val wd:JodiePath = hio.JodiePath(fileContext.getWorkingDirectory) / "hio-tmp"

}
