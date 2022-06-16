package com.asiainfo.ctc.data.neuron

import com.asiainfo.ctc.data.neuron.config.SerDeConfig
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Application {
  @transient private lazy val LOG = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    LOG.info(args.mkString(" "))
    if (args.length < 1) {
      LOG.error("Usage: class <table-id>")
      sys.exit(-1)
    }

    val tableId = args(0)
    val conf = new SparkConf().setAppName("宽表上传-" + tableId)
    conf.set("spark.sql.crossJoin.enabled", "true")
    conf.set("spark.sql.shuffle.partition", "1")

    val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
    import ss.implicits._
    val serDeConfigDF = ss.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://137.32.181.208:8922/dataos_alarm?useSSL=false")
      .option("user", "dataos")
      .option("password", "Dedv_0106sOasR")
      .option("query",
        s"""
          |SELECT
          |  id, table_name as tableName, payload, coalesce
          |FROM tr_bwt_serde_config
          |WHERE id = $tableId""".stripMargin)
      .load()
    val serDeConfig = serDeConfigDF.as[SerDeConfig].first()

    val tableName = serDeConfig.tableName
    val payload = serDeConfig.payload
    val location = serDeConfig.location
    val coalesce = serDeConfig.coalesce

    val df = ss.sql(payload).toDF().coalesce(coalesce)
    df.write
      .format("neuron")
      .option("table", tableName)
      .save(location)

    ss.stop()
  }
}
