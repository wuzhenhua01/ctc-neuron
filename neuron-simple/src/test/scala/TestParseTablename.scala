import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

object TestParseTablename {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("").setMaster("local")
    conf.set("spark.sql.crossJoin.enabled", "true")
    conf.set("spark.sql.shuffle.partition", "1")
    val ss = SparkSession.builder.config(conf).getOrCreate
    import ss.implicits._
    val rdd: RDD[(Int, String)] = ss.sparkContext.makeRDD(List((1, "zhangsan"), (2, "lisi"), (3, "zhangsan"), (4, "zhangsan"), (2, "zhangsan")))
    rdd.toDF("id", "name").createTempView("user")

    val logicalPlan: LogicalPlan = ss.sessionState.sqlParser
      .parsePlan("select * from aa left join bbb")

    val tableName = logicalPlan
      .collectFirst {
        case r: UnresolvedRelation => r.tableName
      }.get

    println(tableName)

  }
}
