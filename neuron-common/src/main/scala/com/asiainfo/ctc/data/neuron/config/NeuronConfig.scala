package com.asiainfo.ctc.data.neuron.config

import java.util.Properties

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
class NeuronConfig {
  var props = new Properties

  def this(props: Properties) {
    this()
    this.props = new Properties(props)
  }
}
