package com.asiainfo.ctc.data.neuron.fs

import java.io.IOException

class HDFSWriterFactory {
  val SequenceFileType = "SequenceFile"
  val DataStreamType = "DataStream"
  val CompStreamType = "CompressedStream"

  def getWriter(fileType: String): HDFSWriter = {
    fileType match {
      case SequenceFileType => new HDFSSequenceFile
      case DataStreamType => new HDFSDataStream
      case CompStreamType => new HDFSCompressedDataStream
      case _ => throw new IOException(s"File type $fileType not supported");
    }
  }
}
