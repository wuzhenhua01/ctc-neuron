package com.asiainfo.ctc.data.neuron.util

import java.io.IOException

import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-08
 */
object FileIOUtils {
  lazy val LOG: Logger = LoggerFactory.getLogger(getClass)

  def createFileInPath(fileSystem: FileSystem, fullPath: Path, content: Array[Byte], ignoreIOE: Boolean = false): Unit = {
    try { // If the path does not exist, create it first
      if (!fileSystem.exists(fullPath) && fileSystem.createNewFile(fullPath))
        LOG.info("Created a new file in path: {}", fullPath)
      else throw new IOException("Failed to create file " + fullPath)

      val fsout = fileSystem.create(fullPath, true)
      fsout.write(content)
      fsout.close()
    } catch {
      case e: IOException =>
        LOG.warn("Failed to create file {}", fullPath)
        if (!ignoreIOE) throw new IOException("Failed to create file " + fullPath, e)
    }
  }
}
