import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object TestDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("宽表上传-")
      .set("spark.sql.shuffle.partition", "4")

    val spark = SparkSession.builder
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("select *from ods_tp.bwt_prd_inst_income_202204").toDF().write.format("neuron").option("table", "ccc").save("tmp/wuzh")
    spark.stop()
  }
}
