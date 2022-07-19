package com.asiainfo.ctc.data.neuron

import com.asiainfo.ctc.data.neuron.config.SerDeConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-17
 */
object Application {
  @transient private lazy val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    LOG.info(args.mkString(" "))
    if (args.length < 2) {
      LOG.error("Usage: class <table-id> <date>")
      sys.exit(-1)
    }

    val tableId = args(0)
    val date = args(1)
    val conf = new SparkConf().setAppName(s"宽表上传-$tableId:$date")
    conf.set("spark.sql.crossJoin.enabled", "true")
    conf.set("spark.sql.shuffle.partition", "1")

    LOG.info("开始获取生成参数...")
    val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
    import ss.implicits._
    val serDeConfigDF = ss.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://137.32.181.208:8922/dataos_alarm?useSSL=false")
      .option("user", "dataos")
      .option("password", "Dedv_0106sOasR")
      .option(
        "query",
        s"""
           |SELECT
           |  id, table_name as tableName, payload, mode, location, coalesce
           |FROM tr_bwt_serde_config
           |WHERE id = $tableId""".stripMargin)
      .load()
    val serDeConfig = serDeConfigDF.as[SerDeConfig].first()

    val tableName = serDeConfig.tableName
    val payload = serDeConfig.payload.replace("?", date)
    val location = serDeConfig.location
    val mode = serDeConfig.mode
    val coalesce = serDeConfig.coalesce
    LOG.info("宽表文件前缀: {}", tableName)
    LOG.info("宽表生成内容: {}", payload)
    LOG.info("宽表生成路径: {}", location)
    LOG.info("宽表写入模式: {}", mode)
    LOG.info("宽表写入并发: {}", coalesce)

    val retrySrc = ss.read
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://137.32.181.208:8922/dataos_pro?useSSL=false")
      .option("user", "dataos")
      .option("password", "Dedv_0106sOasR")
      .option("query",
        s"""
           |SELECT
           |  max(repeat_no)
           |FROM dp_group_log
           |WHERE interface_name = '$tableName'
           |  AND data_date = '$date'""".stripMargin)
      .load()
      .head()
      .getString(0)
    val retry = Option(retrySrc).map(_.toInt).map(_ + 1).getOrElse(0)

    val df = ss.sql(payload).toDF().coalesce(coalesce)
    df.write
      .format("neuron")
      .option("table", tableName)
      .option("date", date)
      .option("retry", retry)
      .mode(mode)
      .save(location)

    LOG.info(s"宽表[$tableId:$date]文件生成结束.")

    LOG.info("开始校验文件记录数...")
    val table = ss.sessionState.sqlParser
      .parsePlan(payload)
      .collectFirst {
        case r: UnresolvedRelation => r.tableName
      }.get
    val srcCount = ss.sql(s"SELECT count(1) FROM ${table}").first().getLong(0)
    LOG.info("原始表记录数:{}", srcCount)

    ss.read
      .option("pathGlobFilter", "*.gz")
      .text(location)
      .createTempView("payload")
    val targetCount = ss.sql("SELECT count(1) FROM payload").first().getLong(0)
    LOG.info("写入记录数:{}", targetCount)

    val rdd = ss.sparkContext.makeRDD(Seq((tableName, date, 1, retrySrc, srcCount, targetCount)))
    rdd.toDF().write
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://137.32.181.208:8922/dataos_pro?useSSL=false")
      .option("user", "dataos")
      .option("password", "Dedv_0106sOasR")
      .option("dbtable", "dp_group_check")
      .mode(SaveMode.Append)
      .save()

    ss.stop()

    if (srcCount != targetCount) {
      LOG.error(s"生成数据量比对不一致, 表记录数:$srcCount -> 生成记录数:$targetCount")
      sys.exit(-1)
    }
  }
}
