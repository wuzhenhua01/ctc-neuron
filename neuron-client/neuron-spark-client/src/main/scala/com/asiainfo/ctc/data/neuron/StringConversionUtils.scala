package com.asiainfo.ctc.data.neuron

import com.asiainfo.ctc.data.neuron.NeuronSparkUtils.sparkAdapter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
object StringConversionUtils {
  def createInternalRowToStringConverter(rootCatalystType: StructType, nullable: Boolean): InternalRow => List[Any] = {
    val serializer = sparkAdapter.createStringSerializer(rootCatalystType, nullable)
    row => serializer
      .serialize(row)
      .asInstanceOf[List[Any]]
  }
}
