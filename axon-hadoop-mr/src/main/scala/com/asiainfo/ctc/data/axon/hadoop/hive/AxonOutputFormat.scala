package com.asiainfo.ctc.data.axon.hadoop.hive

import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.ql.io.HiveOutputFormat
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.io.{Writable, WritableComparable}
import org.apache.hadoop.util.Progressable

class AxonOutputFormat extends HiveOutputFormat[WritableComparable[_], Writable] {
  override def getHiveRecordWriter(jc: Nothing,
                                   outPath: Path,
                                   valueClass: Class[_ <: Writable],
                                   isCompressed: Boolean,
                                   tableProperties: Properties,
                                   progress: Progressable) = {
    var rowSeparator: Int = _
    val rowSeparatorString = tableProperties.getProperty(serdeConstants.LINE_DELIM, "\n")

    val fs = outPath.getFileSystem(jc)

  }
}
