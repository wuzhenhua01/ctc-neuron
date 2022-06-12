package com.asiainfo.ctc.data.neuron.config

import org.apache.hadoop.conf.Configuration;

/**
 * A wrapped configuration which can be serialized.
 */
case class SerializableConfiguration(@transient configuration: Configuration) {
  def get(): Configuration = {
    configuration
  }
}
