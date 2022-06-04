package com.asiainfo.ctc.data.neuron

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object DataSourceUtils {
  def createNeuronRecord(record: List[Any], sepVal: String): String = {
    record.mkString(sepVal)
  }
}
