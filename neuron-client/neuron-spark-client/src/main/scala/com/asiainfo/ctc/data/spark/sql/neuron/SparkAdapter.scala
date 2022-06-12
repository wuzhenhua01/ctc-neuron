package com.asiainfo.ctc.data.spark.sql.neuron

import org.apache.spark.sql.types.DataType

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
trait SparkAdapter {
  def createStringSerializer(rootCatalystType: DataType, nullable: Boolean): NeuronStringSerializer
}
