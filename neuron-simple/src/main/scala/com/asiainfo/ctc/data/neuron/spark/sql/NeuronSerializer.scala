package com.asiainfo.ctc.data.neuron.spark.sql

import org.apache.avro.util.Utf8
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecializedGetters, SpecificInternalRow}
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
class NeuronSerializer(rootCatalystType: DataType, nullable: Boolean) {
  def serialize(catalystData: Any): Any = {
    converter.apply(catalystData)
  }

  private val converter: Any => Any = {
    val baseConverter = rootCatalystType match {
      case st: StructType => newStructConverter(st).asInstanceOf[Any => Any]
      case _ =>
        val tmpRow = new SpecificInternalRow(Seq(rootCatalystType))
        val converter = newConverter(rootCatalystType)
        (data: Any) =>
          tmpRow.update(0, data)
          converter.apply(tmpRow, 0)
    }

    if (nullable) {
      (data: Any) =>
        if (data == null) {
          null
        } else {
          baseConverter.apply(data)
        }
    } else {
      baseConverter
    }
  }

  private type Converter = (SpecializedGetters, Int) => Any

  private def newConverter(catalystType: DataType): Converter = {
    catalystType match {
      case NullType => (_, _) => null
      case BooleanType => (getter, ordinal) => getter.getBoolean(ordinal)
      case ByteType => (getter, ordinal) => getter.getByte(ordinal).toInt
      case ShortType => (getter, ordinal) => getter.getShort(ordinal).toInt
      case IntegerType => (getter, ordinal) => getter.getInt(ordinal)
      case LongType => (getter, ordinal) => getter.getLong(ordinal)
      case FloatType => (getter, ordinal) => getter.getFloat(ordinal)
      case DoubleType => (getter, ordinal) => getter.getDouble(ordinal)
      case d: DecimalType => (getter, ordinal) => getter.getDecimal(ordinal, d.precision, d.scale)
      case StringType => (getter, ordinal) => new Utf8(getter.getUTF8String(ordinal).getBytes)
      case _ =>
        throw new IncompatibleSchemaException(s"Cannot convert Catalyst type $catalystType to type string.")
    }
  }

  private def newStructConverter(catalystStruct: StructType): InternalRow => List[Any] = {
    val fieldConverters = catalystStruct.map(f1 => newConverter(f1.dataType))
    val numFields = catalystStruct.length
    (row: InternalRow) =>
      val result = new ListBuffer[Any]()
      var i = 0
      while (i < numFields) {
        if (row.isNullAt(i)) result += null else result += fieldConverters(i).apply(row, i)
        i += 1
      }
      result.toList
  }
}

private[neuron] class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)
