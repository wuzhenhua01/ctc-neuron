package com.asiainfo.ctc.data.neuron.hadoop.hive

import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter
import org.apache.hadoop.hive.ql.io.HiveOutputFormat
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.io.{Writable, WritableComparable}
import org.apache.hadoop.util.Progressable

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
class AxonOutputFormat extends HiveOutputFormat[WritableComparable[_], Writable] {
  override def getHiveRecordWriter(jc: Nothing, outPath: Path, valueClass: Class[_ <: Writable], isCompressed: Boolean, tableProperties: Properties, progress: Progressable): RecordWriter = {
    var rowSeparator: Int = 0
    val rowSeparatorString = tableProperties.getProperty(serdeConstants.LINE_DELIM, "\n")

    val fs = outPath.getFileSystem(jc)

    new RecordWriter {
      override def write(w: Writable): Unit = {

      }

      override def close(abort: Boolean): Unit = {

      }
    }
  }
}
