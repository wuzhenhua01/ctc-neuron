import java.util.concurrent.locks.LockSupport

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object TestDataSource2 {
  val Log: Logger = LoggerFactory.getLogger(getClass)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("宽表上传-")
      .set("spark.sql.shuffle.partition", "1")

    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    val rdd: RDD[(Int, String)] = spark.sparkContext.makeRDD(List((1, "zhangsan"), (2, "lisi"), (3, "zhangsan"), (4, "zhangsan"), (2, "zhangsan")))
    rdd.toDF("id", "name").createTempView("user")
    spark.sql("select *from user where 1 = 2").toDF().coalesce(1).write.format("neuron")
      .option("date", "20220713")
      .option("retry", "00")
      .option("table", "ccc")
      .mode(SaveMode.Overwrite)
      .save("tmp/wuzh")
    spark.stop()
  }
}
