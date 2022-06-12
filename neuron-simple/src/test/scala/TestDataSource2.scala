import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object TestDataSource2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("宽表上传-")
      .set("spark.sql.shuffle.partition", "1")

    val spark = SparkSession.builder
      .config(conf)
      // .enableHiveSupport()
      .getOrCreate()

    val rdd: RDD[(Int, String)] = spark.sparkContext.makeRDD(List((1, "zhangsan"), (2, "lisi"), (3, "zhangsan"), (4, "zhangsan"), (2, "zhangsan")))
    rdd.groupByKey()
      .foreachPartition {
        case a: (Int, Iterable[String]) =>

      }

    spark.stop()
  }
}
