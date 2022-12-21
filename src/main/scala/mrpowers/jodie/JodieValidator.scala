package mrpowers.jodie

import org.apache.spark.sql.DataFrame

case class JodieValidationError(smth: String) extends Exception(smth)
object JodieValidator {
  def validateColumnsExistsInDataFrame(columns:Seq[String],df:DataFrame):Unit ={
    val dataFrameColumns = df.columns.toSeq
    val noExistingColumns = columns.diff(dataFrameColumns)
    if (noExistingColumns.nonEmpty) {
      throw JodieValidationError(s"these columns: $noExistingColumns do not exists in the dataframe: $dataFrameColumns")
    }
  }
}
