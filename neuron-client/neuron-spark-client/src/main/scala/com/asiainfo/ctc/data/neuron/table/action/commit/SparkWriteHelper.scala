package com.asiainfo.ctc.data.neuron.table.action.commit

import org.apache.spark.rdd.RDD

class SparkWriteHelper private extends BaseWriteHelper[RDD[_]] {

}

object SparkWriteHelper {
  private val instance: SparkWriteHelper = SparkWriteHelper()

  private def apply(): SparkWriteHelper = new SparkWriteHelper()

  def newInstance(): SparkWriteHelper = {
    instance
  }
}