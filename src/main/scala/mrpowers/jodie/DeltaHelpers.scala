package mrpowers.jodie

import org.apache.spark.sql.SparkSession
import io.delta.tables._

object DeltaHelpers {

  /**
   * Gets the latest version of a Delta lake
   */
  def latestVersion(path: String): Long = {
    DeltaTable
      .forPath(SparkSession.active, path)
      .history(1)
      .select("version")
      .head()(0)
      .asInstanceOf[Long]
  }

}
