package com.asiainfo.ctc.data.neuron


import com.asiainfo.ctc.data.neuron.util.{DataSourceUtils, FileIOUtils, NeuronSparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object NeuronSparkSqlWriter {
  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass)

  def write(sqlContext: SQLContext, mode: SaveMode, optParams: Map[String, String], df: DataFrame): Unit = {
    assert(optParams.get("path").exists(StringUtils.isNotBlank), "'path' must be set")
    assert(optParams.get("table").exists(StringUtils.isNotBlank), "'path' must be set")

    val path = optParams("path")
    val tableName = optParams("table")
    val conf = sqlContext.sparkContext.hadoopConfiguration
    val retry = 0

    val maxProcessSize = 1 << 30
    // val maxProcessSize = 10
    // val processSize: LongAccumulator = sqlContext.sparkContext.longAccumulator("processSize")
    // val fileId: LongAccumulator = sqlContext.sparkContext.longAccumulator("fileId")
    //var processSize = 0L
    //var fileId = 0

    val record = NeuronSparkUtils.createRdd(df)

    //    val neuronAllIncomingRecords = record.map(r => DataSourceUtils.createNeuronRecord(r, "\t"))

    /*    neuronAllIncomingRecords
          .coalesce(1)
          .foreachPartition(recordItr => {
            val writer = new BucketWriter(path, tableName, rollSize = 1 << 30)
            recordItr.map(_.getBytes("GBK")).foreach(writer.append)
            writer.close()
          })*/

    val neuronAllIncomingRecords = record
      .map(r => DataSourceUtils.createNeuronRecord(r, "\t"))
      .map(line => line + "\r\n")
      .map(_.getBytes("GBK"))
      .cache()

    val partitionSizeMap = neuronAllIncomingRecords
      .map(_.length)
      .mapPartitionsWithIndex((idx, itr) => {
        val partitionSize = itr.sum
        Iterator((idx, partitionSize))
      })
      .collect()
      .toMap

    println(partitionSizeMap)
    val broadcastPartitionSizeMap: Broadcast[Map[Int, Int]] = sqlContext.sparkContext.broadcast(partitionSizeMap)

    neuronAllIncomingRecords.mapPartitionsWithIndex((idx, itr) => {
      val size = broadcastPartitionSizeMap.value(idx)
      println(idx + " partition size is " + size)
      if (size < maxProcessSize) itr
      else {
        val num: Int = size / maxProcessSize
        var processSize = 0L
        var fileId = 0

        var writer = new _BucketWriter(path, tableName, retry, partitionIndex = idx, fileId = fileId)
        itr.filter {
          record: Array[Byte] =>
            if (fileId > num) true
            else {
              if (processSize > maxProcessSize) {
                fileId += 1
                processSize = 0
                writer.close()
                writer = new _BucketWriter(path, tableName, retry, partitionIndex = idx, fileId = fileId)
              }
              writer.append(record)
              processSize += record.length
              false
            }
        }
        writer.close()
        itr
      }
    }).repartition(1).foreachPartition(recordItr => {
      var processSize = 0L
      var fileId = 0
      var writer = new _BucketWriter(path, tableName, retry, partitionIndex = 0, fileId = fileId)
      recordItr.foreach(record => {
        if (processSize > maxProcessSize) {
          fileId += 1
          processSize = 0
          writer.close()
          writer = new _BucketWriter(path, tableName, retry, partitionIndex = 0, fileId = fileId)
        }
        writer.append(record)
        processSize += record.length
      })
      writer.close()
    })

    LOG.info("Creating check file for {}", tableName)
    val checkPath = new Path(path, f"""$tableName.$retry%02d.000.000.841.CHECK""")
    val fs = checkPath.getFileSystem(conf)
    FileIOUtils.createFileInPath(fs, checkPath, s"""$tableName\r\n""".getBytes())
  }
}
