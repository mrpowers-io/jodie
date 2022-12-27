package hio

import java.nio.charset.StandardCharsets

trait FileContent {
  def apply():Array[Byte]
}

object FileContent{

  private val CHARSET = StandardCharsets.UTF_8
  implicit class FromStringContent(value:String) extends FileContent {
    override def apply(): Array[Byte] = {
      value.getBytes(CHARSET)
    }
  }
}



