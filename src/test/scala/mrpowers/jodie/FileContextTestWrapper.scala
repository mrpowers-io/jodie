package mrpowers.jodie

import hio.JodiePath
import org.apache.hadoop.fs.FileContext

trait FileContextTestWrapper{

  lazy val fileContext: FileContext = FileContext.getLocalFSFileContext
  lazy val wd:JodiePath = {
    val wd = hio.JodiePath(fileContext.getWorkingDirectory) / "hio-tmp"
    hio.mkdir(wd/"dir1")
    hio.mkdir(wd/"dir2")
    hio.mkdir(wd/"dir3")
    hio.write(wd/"dir1/test_data1.txt","hola")
    hio.write(wd/"dir1/test_data2.txt","hola")
    hio.write(wd/"dir1/test_data3.txt","hola")
    wd
  }
}
