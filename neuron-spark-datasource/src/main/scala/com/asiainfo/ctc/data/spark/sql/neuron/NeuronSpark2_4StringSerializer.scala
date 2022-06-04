package com.asiainfo.ctc.data.spark.sql.neuron

import org.apache.spark.sql.types.DataType

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
class NeuronSpark2_4StringSerializer(rootCatalystType: DataType, nullable: Boolean) extends NeuronStringSerializer {
  val neuronSerializer = new NeuronSerializer(rootCatalystType, nullable)

  override def serialize(catalystData: Any) = {
    neuronSerializer.serialize(catalystData)
  }
}
