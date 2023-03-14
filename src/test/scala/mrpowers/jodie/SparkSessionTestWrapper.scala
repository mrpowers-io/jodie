package mrpowers.jodie

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

trait SparkSessionTestWrapper {

  lazy val spark: SparkSession = {
    Logger.getLogger("org").setLevel(Level.OFF)
    SparkSession
      .builder()
      .master("local")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", (os.pwd / "tmp").toString())
      .config( "spark.driver.host", "localhost" )
      .appName("spark session")
      .getOrCreate()
  }
  spark.sparkContext.setLogLevel(Level.OFF.toString)

}
