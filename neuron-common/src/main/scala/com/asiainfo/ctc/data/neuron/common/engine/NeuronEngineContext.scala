package com.asiainfo.ctc.data.neuron.common.engine

import com.asiainfo.ctc.data.neuron.config.SerializableConfiguration

abstract class NeuronEngineContext(private val hadoopConf: SerializableConfiguration) {
  def getHadoopConf: SerializableConfiguration = hadoopConf
}
