package com.asiainfo.ctc.data.neuron.table.action

import com.asiainfo.ctc.data.neuron.common.engine.NeuronEngineContext
import com.asiainfo.ctc.data.neuron.config.NeuronWriteConfig
import com.asiainfo.ctc.data.neuron.table.NeuronTable
import org.apache.hadoop.conf.Configuration

abstract case class BaseActionExecutor[I](@transient context: NeuronEngineContext, config: NeuronWriteConfig, table: NeuronTable[_], instantTime: String) {
  @transient val hadoopConf: Configuration = context.getHadoopConf.get()

  def execute()
}
