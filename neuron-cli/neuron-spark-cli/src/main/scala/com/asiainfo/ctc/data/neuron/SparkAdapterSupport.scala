package com.asiainfo.ctc.data.neuron

import com.asiainfo.ctc.data.spark.sql.neuron.SparkAdapter

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
trait SparkAdapterSupport {
  lazy val sparkAdapter: SparkAdapter = {
    val adapterClass = "com.asiainfo.ctc.data.spark.sql.adapter.Spark2Adapter"
    getClass.getClassLoader.loadClass(adapterClass).newInstance().asInstanceOf[SparkAdapter]
  }
}
