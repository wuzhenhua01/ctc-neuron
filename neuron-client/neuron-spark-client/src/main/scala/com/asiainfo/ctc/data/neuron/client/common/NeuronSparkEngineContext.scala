package com.asiainfo.ctc.data.neuron.client.common

import com.asiainfo.ctc.data.neuron.common.engine.NeuronEngineContext
import com.asiainfo.ctc.data.neuron.config.SerializableConfiguration
import org.apache.spark.SparkContext

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
class NeuronSparkEngineContext(sc: SparkContext) extends NeuronEngineContext(SerializableConfiguration(sc.hadoopConfiguration)) {
}
