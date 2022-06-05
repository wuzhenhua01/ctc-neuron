package com.asiainfo.ctc.data.neuron

import com.asiainfo.ctc.data.neuron.client.SparkRDDWriteClient
import com.asiainfo.ctc.data.neuron.client.common.NeuronSparkEngineContext
import com.asiainfo.ctc.data.neuron.config.NeuronWriteConfig
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object DataSourceUtils {
  def createNeuronRecord(record: List[Any], sepVal: String): String = {
    record.mkString(sepVal)
  }

  def createNeuronClient(sc: SparkContext, bashPath: String, tblName: String): SparkRDDWriteClient = {
    new SparkRDDWriteClient(new NeuronSparkEngineContext(sc))
  }

  def doWriteOperation(client: SparkRDDWriteClient, neuronAllIncomingRecords: RDD[String]) = {

  }

  def createNeuronConfig(basePath: String, tblName: String, parameters: Map[String, String]): NeuronWriteConfig = {
    null
  }
}
