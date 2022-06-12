package com.asiainfo.ctc.data.neuron.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object NeuronSparkUtils {
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
