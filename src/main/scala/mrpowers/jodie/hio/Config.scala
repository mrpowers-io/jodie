package hio

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

object Config {
  def getFSConfiguration:Configuration = {
    val configuration = new Configuration()
    val configPath = Option(System.getenv("CONFIG_PATH"))
    configPath match {
      case Some(pathStr) => configuration.addResource(new Path(pathStr))
      case None =>
    }
    configuration
  }
}
