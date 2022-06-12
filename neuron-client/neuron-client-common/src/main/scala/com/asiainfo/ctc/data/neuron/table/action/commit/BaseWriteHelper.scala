package com.asiainfo.ctc.data.neuron.table.action.commit

import com.asiainfo.ctc.data.neuron.common.engine.NeuronEngineContext
import com.asiainfo.ctc.data.neuron.table.NeuronTable

abstract class BaseWriteHelper[I] {
  def write(instantTime: String, inputRecordsRDD: I, context: NeuronEngineContext, table: NeuronTable[I], executor: BaseCommitActionExecutor[I]): Unit = {
    executor.execute(inputRecordsRDD)
  }
}
