package com.asiainfo.ctc.data.neuron.fs

import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.{CompressionCodec, DefaultCodec}

class HDFSCompressedDataStream extends AbstractHDFSWriter {
  override def open(filePath: String): Unit = {
    val defCodec = new DefaultCodec
    val cType = CompressionType.BLOCK
    open(filePath, defCodec, cType)
  }

  override def open(filePath: String, codec: CompressionCodec, cType: SequenceFile.CompressionType): Unit = {

  }
}
