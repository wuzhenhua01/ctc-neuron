/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object TestDataSource4 {
  def main(args: Array[String]): Unit = {
    val maxProcessSize = 1 << 30
    val partitionSizeMap = Map(0 -> 164706168, 1 -> 164496904)
    val fileIdMap = (Map[Int, IndexedSeq[Long]](-1 -> IndexedSeq(0)) /: partitionSizeMap.mapValues(size => size / maxProcessSize).toSeq.sortWith(_._1 < _._1))((x1, x2) => {
      val latestId = x1.get(x2._1 - 1).filter(_.nonEmpty).getOrElse(IndexedSeq(0L)).max
      val fields = for (incr <- 1L to x2._2) yield latestId + incr
      x1 + ((x2._1, fields))
    })

  }
}
