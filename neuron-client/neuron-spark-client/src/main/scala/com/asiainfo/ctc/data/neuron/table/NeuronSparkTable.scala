package com.asiainfo.ctc.data.neuron.table

import com.asiainfo.ctc.data.neuron.client.common.NeuronSparkEngineContext
import com.asiainfo.ctc.data.neuron.common.engine.NeuronEngineContext
import com.asiainfo.ctc.data.neuron.config.NeuronWriteConfig
import com.asiainfo.ctc.data.neuron.table.action.commit.SparkBulkInsertTableCommitActionExecutor
import org.apache.spark.rdd.RDD

class NeuronSparkTable[T <: RDD[String]](config: NeuronWriteConfig, context: NeuronEngineContext) extends NeuronTable[RDD[_]](config, context) {
  override def insert(context: NeuronEngineContext, instantTime: String, records: RDD[_]): Unit = {
    new SparkBulkInsertTableCommitActionExecutor(context.asInstanceOf[NeuronSparkEngineContext], config, this, instantTime, records).execute()
  }
}

object NeuronSparkTable {
  def apply(config: NeuronWriteConfig, context: NeuronEngineContext): NeuronSparkTable[Nothing] = new NeuronSparkTable[Nothing](config, context)
}
