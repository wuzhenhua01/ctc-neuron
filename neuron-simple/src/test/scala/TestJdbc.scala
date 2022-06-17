import com.asiainfo.ctc.data.neuron.model.NeuronLog
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object TestJdbc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("宽表上传-")
      .set("spark.sql.shuffle.partition", "1")

    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()

    val retry = 0
    val neuronLog = NeuronLog(interface_name = "aaaa",
      data_date = "20220617",
      serial_num = 1.toString,
      repeat_no = f"$retry%02d",
      begin_time = "",
      end_time = "",
      local_path = "/tmp/daf")

    neuronLog.data_row_count = 12
    neuronLog.data_file_num = 12.toString
    neuronLog.check_file_name = "dafda"
    neuronLog.check_file_num = "dsadadfa".length.toString
    spark.sqlContext.createDataFrame(Seq(neuronLog)).write
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://137.32.181.208:8922/dataos_pro?useSSL=false")
      .option("user", "dataos")
      .option("password", "Dedv_0106sOasR")
      .option("dbtable", "dp_group_log")
      .mode(SaveMode.Append)
      .save()
  }
}
