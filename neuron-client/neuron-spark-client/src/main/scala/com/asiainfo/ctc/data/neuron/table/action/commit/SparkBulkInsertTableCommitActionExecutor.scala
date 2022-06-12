package com.asiainfo.ctc.data.neuron.table.action.commit

import com.asiainfo.ctc.data.neuron.client.common.NeuronSparkEngineContext
import com.asiainfo.ctc.data.neuron.config.NeuronWriteConfig
import com.asiainfo.ctc.data.neuron.table.NeuronTable
import org.apache.spark.rdd.RDD

class SparkBulkInsertTableCommitActionExecutor(context: NeuronSparkEngineContext,
                                               config: NeuronWriteConfig,
                                               table: NeuronTable[RDD[_]],
                                               instantTime: String,
                                               inputRecordsRDD: RDD[_]) extends BaseSparkCommitActionExecutor(context, config, table, instantTime) {
  override def execute(): Unit = {
    SparkWriteHelper.newInstance().write(instantTime, inputRecordsRDD, context, table, this)
  }
}
