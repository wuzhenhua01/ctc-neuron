import scala.collection.immutable

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object TestDataSource3 {
  def main(args: Array[String]): Unit = {
    val maxProcessSize = 1 << 30
    val partitionSizeMap = Map(
      0 -> 1458861894,
      5 -> 1739640968,
      10 -> 1878543132,
      14 -> 2002686929,
      1 -> 1185950148,
      6 -> 1143516929,
      9 -> 1860198889,
      13 -> 1549446935,
      2 -> 2014426846,
      17 -> 1370129847,
      12 -> 1871119524,
      7 -> 1683127512,
      3 -> 1662921606,
      18 -> 1733361265,
      16 -> 1461739972,
      11 -> 2055259795,
      8 -> 1359284628,
      19 -> 1534038126,
      4 -> 1839977656,
      15 -> 1732023708)

    println(partitionSizeMap.mapValues(size => size / maxProcessSize).toSeq.sortWith(_._1 < _._1))

    val fileIdMap: Map[Int, IndexedSeq[Long]] = partitionSizeMap.mapValues(size => size / maxProcessSize).toSeq.sortWith(_._1 < _._1).foldLeft(Map[Int, IndexedSeq[Long]](-1 -> IndexedSeq(0)))((x1, x2) => {
      val latestId = x1.getOrElse(x2._1 - 1, IndexedSeq(0L)).max
      val fields = for (incr <- 1L to x2._2) yield latestId + incr
      x1 + ((x2._1, fields))
    })
    println(fileIdMap)
    println(fileIdMap(1).isDefinedAt(1))
  }
}
