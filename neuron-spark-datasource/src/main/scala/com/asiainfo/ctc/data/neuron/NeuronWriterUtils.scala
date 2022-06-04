package com.asiainfo.ctc.data.neuron

import java.util.Properties

import com.asiainfo.ctc.data.neuron.config.NeuronConfig

import scala.collection.JavaConversions.mapAsJavaMap

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object NeuronWriterUtils {
  def parametersWithWriteDefaults(parameters: Map[String, String]): Map[String, String] = {
    val props = new Properties()
    props.putAll(parameters)

    Map()
  }

  def convertMapToNeuronConfig(parameters: Map[String, String]): NeuronConfig = {
    val properties = new Properties()
    properties.putAll(parameters)
    new NeuronConfig(properties)
  }
}
