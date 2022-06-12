package com.asiainfo.ctc.data.neuron.client

import com.asiainfo.ctc.data.neuron.client.common.NeuronSparkEngineContext
import com.asiainfo.ctc.data.neuron.config.NeuronWriteConfig
import com.asiainfo.ctc.data.neuron.table.{NeuronSparkTable, NeuronTable}
import org.apache.spark.rdd.RDD

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
class SparkRDDWriteClient(context: NeuronSparkEngineContext, config: NeuronWriteConfig) extends BaseNeuronWriterClient(context) {
  def postWrite(instantTime: String): Unit = {

  }

  def insert(records: RDD[String], instantTime: String) = {
    val table: NeuronTable[RDD[String]] = initTable().asInstanceOf[NeuronTable[RDD[String]]]
    // 更新重传号
    preWrite(instantTime)
    table.insert(context, instantTime, records)
    // 生成 check 文件
    postWrite(instantTime)
  }

  override def doInitTable(): NeuronSparkTable[_] = {
    NeuronSparkTable(config, context)
  }
}
