package com.asiainfo.ctc.data.neuron

import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

object NeuronSparkSqlWriter {
  private val LOG = LogManager.getLogger(getClass)

  def write(sqlContext: SQLContext, mode: SaveMode, optParams: Map[String, String], df: DataFrame): Unit = {
    val path = optParams("path")
    val basePath = new Path(path)
    val sparkContext = sqlContext.sparkContext
    val fs = basePath.getFileSystem(sparkContext.hadoopConfiguration)
    df.rdd
    df.queryExecution.toRdd.mapPartitions { rows =>

    }
  }
}
