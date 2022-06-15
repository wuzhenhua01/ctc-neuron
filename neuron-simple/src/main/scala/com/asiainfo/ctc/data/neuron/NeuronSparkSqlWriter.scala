package com.asiainfo.ctc.data.neuron

import com.asiainfo.ctc.data.neuron.util.{DataSourceUtils, FileIOUtils, NeuronSparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object NeuronSparkSqlWriter {
  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass)

  def write(sqlContext: SQLContext, optParams: Map[String, String], df: DataFrame): Unit = {
    assert(optParams.get("path").exists(StringUtils.isNotBlank), "'path' must be set")
    assert(optParams.get("table").exists(StringUtils.isNotBlank), "'path' must be set")

    val path = optParams("path")
    val tableName = optParams("table")
    val conf = sqlContext.sparkContext.hadoopConfiguration
    // 重传号
    val retry = 0

    val maxProcessSize = 1 << 30
    val record = NeuronSparkUtils.createRdd(df)

    val neuronAllIncomingRecords = record
      .map(r => DataSourceUtils.createNeuronRecord(r, "\t"))
      .map(line => line + "\r\n")
      .map(_.getBytes("GBK"))
      .cache()

    val partitionSizeMap = neuronAllIncomingRecords
      .map(_.length.asInstanceOf[Long])
      .mapPartitionsWithIndex((idx, itr) => {
        val partitionSize = itr.sum
        Iterator((idx, partitionSize))
      })
      .collect()
      .toMap

    val fileIdMap = (Map[Int, IndexedSeq[Long]](-1 -> IndexedSeq(0)) /: partitionSizeMap.mapValues(size => size / maxProcessSize)) ((x1, x2) => {
      val latestId = x1.getOrElse(x2._1 - 1, IndexedSeq(0L)).max
      val fields = for (incr <- 1L to x2._2) yield latestId + incr
      x1 + ((x2._1, fields))
    })

    val broadcastPartitionSizeMap = sqlContext.sparkContext.broadcast(partitionSizeMap)
    val broadcastFileIdMap = sqlContext.sparkContext.broadcast(fileIdMap)
    println(partitionSizeMap)
    println(fileIdMap)

    neuronAllIncomingRecords.mapPartitionsWithIndex((idx, itr) => {
      val size = broadcastPartitionSizeMap.value(idx)
      val fileIds = broadcastFileIdMap.value(idx)
      if (size < maxProcessSize) itr
      else {
        var processSize = 0L
        var fileIdIdx = 0
        var writer = new BucketWriter(path, tableName, retry, fileId = fileIds(fileIdIdx).toInt)
        val remain = itr.filter {
          record: Array[Byte] =>
            if (!fileIds.isDefinedAt(fileIdIdx)) true
            else {
              if (processSize > maxProcessSize) {
                fileIdIdx += 1
                processSize = 0
                writer.close()
                writer = new BucketWriter(path, tableName, retry, fileId = fileIds(fileIdIdx).toInt)
              }
              writer.append(record)
              processSize += record.length
              false
            }
        }
        writer.close()
        remain
      }
    }).coalesce(1, shuffle = true).foreachPartition(recordItr => {
      var processSize = 0L
      var fileId = broadcastFileIdMap.value.filter(_._2 nonEmpty).max(Ordering.by[(Int, IndexedSeq[Long]), Int](_._1))._2.max.toInt + 1
      var writer = new BucketWriter(path, tableName, retry, fileId = fileId)
      recordItr.foreach(record => {
        if (processSize > maxProcessSize) {
          fileId += 1
          processSize = 0
          writer.close()
          writer = new BucketWriter(path, tableName, retry, fileId = fileId)
        }
        writer.append(record)
        processSize += record.length
      })
      writer.close()
    })

    LOG.info("Creating check file for {}", tableName)
    val checkPath = new Path(path, f"$tableName.$retry%02d.000.000.841.CHECK")
    val fs = checkPath.getFileSystem(conf)
    FileIOUtils.createFileInPath(fs, checkPath, s"$tableName\r\n".getBytes())
  }
}
