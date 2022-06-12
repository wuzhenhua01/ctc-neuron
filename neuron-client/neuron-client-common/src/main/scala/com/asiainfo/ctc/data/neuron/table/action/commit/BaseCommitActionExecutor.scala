package com.asiainfo.ctc.data.neuron.table.action.commit


import com.asiainfo.ctc.data.neuron.client.WriteStatus
import com.asiainfo.ctc.data.neuron.common.engine.NeuronEngineContext
import com.asiainfo.ctc.data.neuron.config.NeuronWriteConfig
import com.asiainfo.ctc.data.neuron.table.NeuronTable
import com.asiainfo.ctc.data.neuron.table.action.BaseActionExecutor

abstract class BaseCommitActionExecutor[I](context: NeuronEngineContext, config: NeuronWriteConfig, table: NeuronTable[_], instantTime: String) extends BaseActionExecutor(context, config, table, instantTime) {
  def execute(inputRecords: I)

  def handleInsert(idPfx: String, recordItr: Iterator[_]): Iterator[List[WriteStatus]]
}
