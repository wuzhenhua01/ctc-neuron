package com.asiainfo.ctc.data.neuron

import org.apache.spark.SPARK_VERSION
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object NeuronSparkUtils extends SparkAdapterSupport {
  def isSpark2: Boolean = SPARK_VERSION.startsWith("2.")

  def isSpark3: Boolean = SPARK_VERSION.startsWith("3.")

  def isSpark3_0: Boolean = SPARK_VERSION.startsWith("3.0")

  def isSpark3_1: Boolean = SPARK_VERSION.startsWith("3.1")

  def isSpark3_2: Boolean = SPARK_VERSION.startsWith("3.2")


  def createRdd(df: DataFrame): RDD[List[Any]] = {
    val writerSchema = df.schema

    df.queryExecution.toRdd.mapPartitions { rows =>
      if (rows.isEmpty) {
        Iterator.empty
      } else {
        val convert = StringConversionUtils.createInternalRowToStringConverter(writerSchema, nullable = false)
        rows.map { ir => convert(ir) }
      }
    }
  }
}
