package com.asiainfo.ctc.data.neuron

import com.asiainfo.ctc.data.neuron.config.NeuronConfig
import com.asiainfo.ctc.data.neuron.table.NeuronTableConfig
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.{CodecPool, CompressionCodec, CompressionCodecFactory, Compressor}
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object NeuronSparkSqlWriter {
  private lazy val LOG = LogManager.getLogger(getClass)
  private var tableExists: Boolean = false // 重传号是否存在

  def write(sqlContext: SQLContext, mode: SaveMode, optParams: Map[String, String], df: DataFrame): Unit = {
    assert(optParams.get("path").exists(StringUtils.isNotBlank), "'path' must be set")

    val path = optParams("path")
    val basePath = new Path(path)
    val sparkContext = sqlContext.sparkContext

    val record: RDD[List[Any]] = NeuronSparkUtils.createRdd(df)
    val neuronAllIncomingRecords = record.map(r => DataSourceUtils.createNeuronRecord(r, "\t"))


    val client = DataSourceUtils.createNeuronClient(sparkContext, path, "")
    client.startCommit()
    DataSourceUtils.doWriteOperation(client, neuronAllIncomingRecords)

    neuronAllIncomingRecords.foreachPartition { recordItr =>
      val fs = basePath.getFileSystem(new Configuration)
      val codec: CompressionCodec = getCodec("gzip") match {
        case Success(c) => c
        case Failure(ex) => {
          LOG.error("===")
          sys.exit(-2)
        }
      }

      val fsOut = fs.create(basePath)
      val compressor: Compressor = CodecPool.getCompressor(codec, fs.getConf)
      val cmpOut = codec.createOutputStream(fsOut, compressor)
      recordItr.foreach { line =>
        cmpOut.write(line.getBytes("GBK"))
        cmpOut.write('\n')
      }
      CodecPool.returnCompressor(compressor)
    }

  }

  private def handleSaveModes(spark: SparkSession, mode: SaveMode, tablePath: Path, fs: FileSystem) {
    if (mode == SaveMode.Overwrite) {
      fs.delete(tablePath, true)
    }
  }

  private def mergeParamsAndGetNeuronConfig(optParams: Map[String, String], tableConfig: NeuronTableConfig): NeuronConfig = {
    NeuronWriterUtils.convertMapToNeuronConfig(optParams)
  }

  def codecMatches(cls: Class[_ <: CompressionCodec], codecName: String): Boolean = {
    val simpleName = cls.getSimpleName
    if (cls.getName == codecName || simpleName.equalsIgnoreCase(codecName)) return true
    if (simpleName.endsWith("Codec")) {
      val prefix = simpleName.dropRight("Codec".length)
      if (prefix.equalsIgnoreCase(codecName)) return true
    }
    false
  }

  def getCodec(codecName: String): Try[CompressionCodec] = Try {
    val conf = new Configuration
    val codecs = CompressionCodecFactory.getCodecClasses(conf)
    // Wish we could base this on DefaultCodec but appears not all codec's
    // extend DefaultCodec(Lzo)
    var codec: CompressionCodec = null
    val codecStrs = ArrayBuffer("None")
    for (cls <- codecs) {
      codecStrs += cls.getSimpleName
      if (codecMatches(cls, codecName))
        codec = cls.newInstance()
    }

    if (codec == null && !codecName.equalsIgnoreCase("None")) throw new IllegalArgumentException(s"Unsupported compression codec $codecName.  Please choose from: $codecStrs")
    codec
  }
}