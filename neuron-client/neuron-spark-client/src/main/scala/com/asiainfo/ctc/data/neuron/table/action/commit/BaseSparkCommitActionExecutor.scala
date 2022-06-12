package com.asiainfo.ctc.data.neuron.table.action.commit

import com.asiainfo.ctc.data.neuron.client.WriteStatus
import com.asiainfo.ctc.data.neuron.common.engine.NeuronEngineContext
import com.asiainfo.ctc.data.neuron.config.NeuronWriteConfig
import com.asiainfo.ctc.data.neuron.table.NeuronTable
import org.apache.spark.rdd.RDD

abstract class BaseSparkCommitActionExecutor(context: NeuronEngineContext, config: NeuronWriteConfig, table: NeuronTable[_], instantTime: String) extends BaseCommitActionExecutor[RDD[_]](context, config, table, instantTime) {
  override def execute(inputRecords: RDD[_]): Unit = {
    inputRecords.mapPartitions((recordItr: Iterator[_]) => {
      handleInsert(instantTime, recordItr)
    })
  }

  override def handleInsert(idPfx: String, recordItr: Iterator[_]): Iterator[List[WriteStatus]] = {
    null
  }
}
