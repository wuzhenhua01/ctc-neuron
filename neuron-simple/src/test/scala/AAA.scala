import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.io.File

object AAA {
  def main(args: Array[String]): Unit = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val fileSQL: File = File("D:\\ctc-neuron\\neuron-simple\\src\\test\\resources\\1.csv")
    val fileName: File = File("D:\\ctc-neuron\\neuron-simple\\src\\test\\resources\\2.csv")
    val fileNames = fileName.lines()
      .map(_.replaceAll("\"\"", "\""))
      .map(_.drop(1))
      .map(_.dropRight(1))
      .map(mapper.readValue(_, classOf[Map[String, Any]]))
      .map(_ ("mainArgs"))
      .map(_.asInstanceOf[Map[String, String]])
      .map(_ ("checkProfile"))
      .map(_.split('.'))
      .map(_ (0))
      .toSeq

    val fileSQLs = fileSQL.lines()
      .map(_.replaceAll("\"\"", "\""))
      .map(_.drop(1))
      .map(_.dropRight(1))
      .map(mapper.readValue(_, classOf[Map[String, Any]]))
      .map(_ ("mainArgs"))
      .map(_.asInstanceOf[Map[String, String]])
      .map(_ ("extractSql"))
      .map(_.replaceAll("\t", " "))
      .map(_.replaceAll("\r\n", " "))
      .map(_.replaceAll("\n", " "))
      .map(_.replaceAll("[ ]+", " "))
      .map(_.replaceAll(", ", ","))
      .map(_.replaceAll(",", ", "))
      .map(_.toLowerCase)
      .map(_.replaceAll("select", "SELECT"))
      .map(_.replaceAll("from", "FROM"))
      .map(_.replaceAll("_\\(yyyymmdd\\)$", " WHERE stat_date = ?"))
      .map(_.replaceAll("_\\(yyyymm\\)$", " WHERE stat_month = ?"))
      .map(_.replaceAll("ods\\.", "db_tp\\."))
      .map(_.replaceAll("ods_dim\\.", "db_dim\\."))
      .toSeq
    fileNames.zip(fileSQLs)
      .map(t => s"""INSERT INTO tr_bwt_serde_config(table_name, payload, location) VALUES('${t._1}', "${t._2}", '/odsDW/db_tp/bwt_data/${t._1.toLowerCase()}');""")
      .foreach(println)
  }
}
