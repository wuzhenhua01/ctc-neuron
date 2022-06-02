package com.asiainfo.ctc.data.neuron

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

class NeuronEmptyRelation(val sqlContext: SQLContext, val userSchema: StructType) extends BaseRelation {
  override def schema: StructType = userSchema
}
