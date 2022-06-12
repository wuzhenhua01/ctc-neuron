package com.asiainfo.ctc.data.neuron.model

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
class NeuronEmptyRelation(val sqlContext: SQLContext, val userSchema: StructType) extends BaseRelation {
  override def schema: StructType = userSchema
}
