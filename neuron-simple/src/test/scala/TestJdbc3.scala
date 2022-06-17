import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TestJdbc3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("宽表上传-")
      .set("spark.sql.shuffle.partition", "1")

    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()

    val retrySrc = spark.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://137.32.181.208:8922/dataos_pro?useSSL=false")
      .option("user", "dataos")
      .option("password", "Dedv_0106sOasR")
      .option(
        "query",
        s"""
           |SELECT
           |  max(repeat_no)
           |FROM dp_group_log
           |WHERE interface_name = 'BWT_PRD_PO_INST_D'
           |  AND data_date = '202206141'""".stripMargin)
      .load()
      .head()
      .getString(0)
    val retry = Option(retrySrc).map(_.toInt).map(_ + 1).getOrElse(0)

    println(retry)

  }
}
