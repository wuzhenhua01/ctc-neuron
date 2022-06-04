package com.asiainfo.ctc.data.neuron.fs
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.compress.CompressionCodec

class HDFSSequenceFile extends AbstractHDFSWriter {
  override def open(filePath: String): Unit = {

  }

  override def open(filePath: String, codec: CompressionCodec, cType: SequenceFile.CompressionType): Unit = {

  }
}
