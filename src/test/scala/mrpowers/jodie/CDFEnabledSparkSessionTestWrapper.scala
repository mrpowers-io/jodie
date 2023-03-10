package mrpowers.jodie

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait CDFEnabledSparkSessionTestWrapper extends Serializable {
  val spark: SparkSession = {
    val conf: SparkConf = new SparkConf()
    conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    SparkSession.builder().master("local")
      .config(conf)
      .appName("spark session").getOrCreate()
  }
  spark.sparkContext.setLogLevel("OFF")
}