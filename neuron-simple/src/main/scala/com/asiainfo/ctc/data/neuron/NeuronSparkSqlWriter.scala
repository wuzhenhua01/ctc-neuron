package com.asiainfo.ctc.data.neuron

import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

import com.asiainfo.ctc.data.neuron.model.{NeuronLog, NeuronLogDetail}
import com.asiainfo.ctc.data.neuron.util.{DataSourceUtils, FileIOUtils, NeuronSparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.convert.ImplicitConversions._

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object NeuronSparkSqlWriter {
  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass)

  def write(sqlContext: SQLContext, mode: SaveMode, optParams: Map[String, String], df: DataFrame): Unit = {
    assert(optParams.get("path").exists(StringUtils.isNotBlank), "'path' must be set")
    assert(optParams.get("table").exists(StringUtils.isNotBlank), "'table' must be set")
    assert(optParams.get("date").exists(StringUtils.isNotBlank), "'date' must be set")
    assert(optParams.get("retry").exists(StringUtils.isNotBlank), "'retry' must be set")

    val path = optParams("path")
    val basePath = new Path(path)
    val sparkContext = sqlContext.sparkContext
    val tableName = optParams("table")
    val date = optParams("date")
    val fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    val retry = optParams("retry").toInt

    val neuronLog = NeuronLog(interface_name = tableName,
      data_date = date,
      serial_num = 1.toString,
      repeat_no = f"$retry%02d",
      local_path = path)
    val neuronLogUUID = neuronLog.uuid

    LOG.info("开始处理写入模式{}...", mode)
    handleSaveModes(mode, basePath, tableName, fs)
    LOG.info("写入模式{}处理成功.", mode)

    val maxProcessSize = 1 << 30
    val record = NeuronSparkUtils.createRdd(df)

    val neuronAllIncomingRecords = record
      .map(r => DataSourceUtils.createNeuronRecord(r, "\t"))
      .map(line => line + "\r\n")
      .map(_.getBytes("GBK"))
      .cache()

    LOG.info("开始计算分区大小...")
    val partitionSizeMap = neuronAllIncomingRecords
      .map(_.length.asInstanceOf[Long])
      .mapPartitionsWithIndex((idx, itr) => {
        val partitionSize = itr.sum
        Iterator((idx, partitionSize))
      })
      .collect()
      .toMap
    LOG.info("{}.", partitionSizeMap)

    LOG.info("开始计算分区文件id...")
    val fileIdMap = (Map[Int, IndexedSeq[Long]](-1 -> IndexedSeq(0)) /: partitionSizeMap.mapValues(size => size / maxProcessSize).toSeq.sortWith(_._1 < _._1)) ((x1, x2) => {
      val latestId = x1.getOrElse(x2._1 - 1, IndexedSeq(0L)).max
      val fields = for (incr <- 1L to x2._2) yield latestId + incr
      x1 + ((x2._1, fields))
    })
    LOG.info("{}.", fileIdMap)

    val maxFileId = fileIdMap.filter(_._2 nonEmpty).max(Ordering.by[(Int, IndexedSeq[Long]), Int](_._1))._2.max.toInt + 1

    val broadcastPartitionSizeMap = sqlContext.sparkContext.broadcast(partitionSizeMap)
    val broadcastFileIdMap = sqlContext.sparkContext.broadcast(fileIdMap)
    val broadcastMaxFileId = sqlContext.sparkContext.broadcast(maxFileId)
    val accumulatorRecordNum = sqlContext.sparkContext.longAccumulator("记录条数")
    val accumulatorRecordSize = sqlContext.sparkContext.longAccumulator("记录大小")
    val accumulatorFiles = sqlContext.sparkContext.collectionAccumulator[String]("文件集合")
    val collectFileName = (dataFileName:String) => {
      accumulatorFiles.add(dataFileName)
    }

    val createDate = DateTimeFormatter.BASIC_ISO_DATE.format(LocalDate.now())
    LOG.info("开始生成文件...")
    neuronAllIncomingRecords.mapPartitionsWithIndex((idx, itr) => {
      val size = broadcastPartitionSizeMap.value(idx)
      val fileIds = broadcastFileIdMap.value(idx)
      if (size < maxProcessSize) itr
      else {
        var processSize = 0L
        var fileIdIdx = 0
        var writer = new BucketWriter(path, tableName, createDate, date, retry, fileId = fileIds(fileIdIdx).toInt, callback = collectFileName)
        val remain = itr.filter {
          record: Array[Byte] =>
            if (!fileIds.isDefinedAt(fileIdIdx) || ((processSize > maxProcessSize) && !fileIds.isDefinedAt(fileIdIdx + 1))) true
            else {
              if (processSize > maxProcessSize) {
                fileIdIdx += 1
                processSize = 0
                writer.close()
                writer = new BucketWriter(path, tableName, createDate, date, retry, fileId = fileIds(fileIdIdx).toInt, callback = collectFileName)
              }
              writer.append(record)
              processSize += record.length
              accumulatorRecordNum.add(1)
              accumulatorRecordSize.add(record.length)
              false
            }
        }.toList
        writer.close()
        remain.iterator
      }
    }).coalesce(1, shuffle = true).foreachPartition(recordItr => {
      var processSize = 0L
      var fileId = broadcastMaxFileId.value
      var writer = new BucketWriter(path, tableName, createDate, date, retry, fileId = fileId, callback = collectFileName)
      recordItr.foreach(record => {
        if (processSize > maxProcessSize) {
          fileId += 1
          processSize = 0
          writer.close()
          writer = new BucketWriter(path, tableName, createDate, date, retry, fileId = fileId, callback = collectFileName)
        }
        writer.append(record)
        processSize += record.length
        accumulatorRecordNum.add(1)
        accumulatorRecordSize.add(record.length)
      })
      writer.close()
    })
    LOG.info("生成文件成功.")

    LOG.info("开始创建{} Check文件...", tableName)
    val checkPath = new Path(path, f"$tableName.$createDate.$date.$retry%02d.000.000.841.CHECK")
    val checkFilePayload = s"$tableName\r\n".getBytes
    FileIOUtils.createFileInPath(fs, checkPath, checkFilePayload)
    LOG.info("创建{} Check文件成功.", tableName)

    val recordNum = accumulatorRecordNum.value
    val recordSize = accumulatorRecordSize.value

    val neuronLoglogDetail = accumulatorFiles.value.map(NeuronLogDetail(neuronLogUUID, tableName, _))
    sqlContext.createDataFrame(neuronLoglogDetail).write
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://137.32.181.208:8922/dataos_pro?useSSL=false")
      .option("user", "dataos")
      .option("password", "Dedv_0106sOasR")
      .option("dbtable", "dp_group_detail")
      .mode(SaveMode.Append)
      .save()

    neuronLog.end_time = DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(LocalDateTime.now()).replace('T', ' ')
    neuronLog.data_row_count = recordNum
    neuronLog.data_file_num = recordSize match {
      case recordSize if recordSize > (1 << 30) => (recordSize >> 30) + "GB"
      case recordSize if recordSize < (1 << 20) => (recordSize >> 10) + "B"
      case recordSize => (recordSize >> 20) + "KB"
    }
    neuronLog.check_file_name = checkPath.getName
    neuronLog.check_file_num = checkFilePayload.length + "B"
    sqlContext.createDataFrame(Seq(neuronLog)).write
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://137.32.181.208:8922/dataos_pro?useSSL=false")
      .option("user", "dataos")
      .option("password", "Dedv_0106sOasR")
      .option("dbtable", "dp_group_log")
      .mode(SaveMode.Append)
      .save()
  }

  def handleSaveModes(mode: SaveMode, basePath: Path, tableName: String, fs: FileSystem): Unit = {
    LOG.info("开始清理[{}]...", basePath)
    if (mode == SaveMode.Overwrite) {
      fs.delete(basePath, true)
    }
    LOG.info("[{}]清理完成.", basePath)
  }
}
