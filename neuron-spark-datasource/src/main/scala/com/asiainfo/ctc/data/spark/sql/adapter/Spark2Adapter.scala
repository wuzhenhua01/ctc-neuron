package com.asiainfo.ctc.data.spark.sql.adapter

import com.asiainfo.ctc.data.spark.sql.neuron.{NeuronSpark2_4StringSerializer, SparkAdapter}
import org.apache.spark.sql.types.DataType

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
class Spark2Adapter extends SparkAdapter {
  override def createStringSerializer(rootCatalystType: DataType, nullable: Boolean) = {
    new NeuronSpark2_4StringSerializer(rootCatalystType, nullable)
  }
}
