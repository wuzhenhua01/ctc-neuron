package com.asiainfo.ctc.data.neuron.table

import com.asiainfo.ctc.data.neuron.common.engine.NeuronEngineContext
import com.asiainfo.ctc.data.neuron.config.{NeuronWriteConfig, SerializableConfiguration}

abstract case class NeuronTable[I](config: NeuronWriteConfig, @transient context: NeuronEngineContext) {
  def insert(context: NeuronEngineContext, instantTime: String, records: I)

  protected var hadoopConfiguration: SerializableConfiguration = context.getHadoopConf
}



