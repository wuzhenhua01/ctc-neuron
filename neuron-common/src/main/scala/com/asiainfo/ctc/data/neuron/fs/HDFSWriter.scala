package com.asiainfo.ctc.data.neuron.fs

import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.CompressionCodec

trait HDFSWriter {
  def open(filePath: String): Unit

  def open(filePath: String, codec: CompressionCodec, cType: CompressionType): Unit
}
