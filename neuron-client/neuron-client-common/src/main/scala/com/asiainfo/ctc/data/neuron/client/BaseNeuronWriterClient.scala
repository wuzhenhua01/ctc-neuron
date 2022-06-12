package com.asiainfo.ctc.data.neuron.client

import com.asiainfo.ctc.data.neuron.common.engine.NeuronEngineContext
import com.asiainfo.ctc.data.neuron.table.NeuronTable

abstract class BaseNeuronWriterClient(context: NeuronEngineContext) {
  // 重传号
  def startCommit(): String = {
    ""
  }

  def startCommitWithTime(): Unit = {
  }

  protected def preWrite(instantTime: String): Unit = {

  }

  def doInitTable(): NeuronTable[_]

  def initTable(): NeuronTable[_] = {
    val table: NeuronTable[_] = doInitTable()
    table
  }
}
