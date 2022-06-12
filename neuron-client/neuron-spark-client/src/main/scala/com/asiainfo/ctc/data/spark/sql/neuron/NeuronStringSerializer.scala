package com.asiainfo.ctc.data.spark.sql.neuron

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
trait NeuronStringSerializer {
  def serialize(catalystData: Any): Any
}
