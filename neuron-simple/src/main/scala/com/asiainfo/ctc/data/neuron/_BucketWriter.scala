package com.asiainfo.ctc.data.neuron

import java.io.IOException

import com.asiainfo.ctc.data.neuron.util.FileIOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.hadoop.io.compress._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-09
 */
class _BucketWriter(filePath: String, fileName: String, retry: Int = 0, batch: String = "001", partitionIndex: Int = 0, fileId: Int, fileSuffix: String = "gz") {
  private lazy val LOG: Logger = LoggerFactory.getLogger(classOf[_BucketWriter])
  val conf = new Configuration
  conf.setBoolean("fs.automatic.close", false)

  var dataFileName: String = _
  var valFileName: String = _
  var fsOut: FSDataOutputStream = _
  var cmpOut: CompressionOutputStream = _

  var recordCounter = 0L
  var processSize = 0L

  var isOpen = false

  def append(record: Array[Byte]): Unit = {
    if (!isOpen) open()

    cmpOut.write(record)

    recordCounter += 1
    processSize += record.length

    if (recordCounter % 1000000 == 0) {
      LOG.info("write {} records in {}", recordCounter, dataFileName)
      cmpOut.flush()
      fsOut.flush()
    }
  }


  def resetCounters(): Unit = {
    recordCounter = 0
    processSize = 0
  }

  def open(): Unit = {
    if (filePath == null) throw new IOException("Invalid file settings")

    dataFileName = f"""$fileName.$retry%02d.$batch.$partitionIndex.$fileId%03d.841.DATA.$fileSuffix"""
    valFileName = f"""$fileName.$retry%02d.$batch.$partitionIndex.$fileId%03d.841.VAL"""

    val dstPath = new Path(filePath, dataFileName)
    val fs = dstPath.getFileSystem(conf)
    fsOut = if (conf.getBoolean("hdfs.append.support", false) && fs.exists(dstPath)) {
      fs.append(dstPath)
    } else fs.create(dstPath)

    val codec = getCodec("gzip")
    codec.asInstanceOf[DefaultCodec].setConf(conf)
    val compressor = CodecPool.getCompressor(codec, conf)
    cmpOut = codec.createOutputStream(fsOut, compressor)

    resetCounters()
    isOpen = true
  }

  def close(): Unit = {
    if (isOpen) {
      cmpOut.close()
      fsOut.close()
      isOpen = false
      createValFile()
    }
  }

  private def createValFile(): Unit = {
    LOG.info("Creating check file for {}", dataFileName)
    val valPath = new Path(filePath, valFileName)
    val fs = valPath.getFileSystem(conf)
    FileIOUtils.createFileInPath(fs, valPath, s"$dataFileName\t$processSize\t$recordCounter\r\n".getBytes)
  }

  private def codecMatches(cls: Class[_ <: CompressionCodec], codecName: String): Boolean = {
    val simpleName = cls.getSimpleName
    if (cls.getName == codecName || simpleName.equalsIgnoreCase(codecName)) return true
    if (simpleName.endsWith("Codec")) {
      val prefix = simpleName.dropRight("Codec".length)
      if (prefix.equalsIgnoreCase(codecName)) return true
    }
    false
  }

  private def getCodec(codecName: String): CompressionCodec = {
    val conf = new Configuration
    val codecs = CompressionCodecFactory.getCodecClasses(conf)
    // Wish we could base this on DefaultCodec but appears not all codec's
    // extend DefaultCodec(Lzo)
    var codec: CompressionCodec = null
    val codecStrs = ListBuffer[String]("None")
    for (cls <- codecs) {
      codecStrs += cls.getSimpleName
      if (codecMatches(cls, codecName)) {
        codec = cls.newInstance()
      }
    }
    if (codec == null) if (!codecName.equalsIgnoreCase("None"))
      throw new IllegalArgumentException("Unsupported compression codec " + codecName + ".  Please choose from: " + codecStrs)
    codec
  }
}