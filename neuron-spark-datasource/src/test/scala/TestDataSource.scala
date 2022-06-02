import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TestDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("test")
      .set("spark.sql.shuffle.partition", "1")

    val ss = SparkSession.builder
      .config(conf)
      .getOrCreate()
    import ss.implicits._

    val rdd: RDD[(Int, String, Int)] = ss.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df = rdd.toDF("id", "name", "age")
    df.createTempView("user")
    ss.sql("select *from user")
      .toDF()
      .coalesce(1)
      .write
      .format("neuron")
      .mode(SaveMode.Overwrite)
      .save("tmp")
    ss.stop()
  }
}
