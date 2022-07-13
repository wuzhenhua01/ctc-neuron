package com.asiainfo.ctc.data.neuron.mover

import java.sql.DriverManager

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, GlobFilter, Path}
import org.apache.hadoop.io.IOUtils
import org.slf4j.LoggerFactory

object MoverTask {
  @transient private lazy val LOG = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      LOG.error("Usage: class <table-id> <date>")
      sys.exit(-1)
    }

    val tableId = args(0)
    val date = args(1)

    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection("jdbc:mysql://137.32.181.208:8922/dataos_alarm?useSSL=false", "dataos", "Dedv_0106sOasR")
    val ptmt = conn.prepareStatement("SELECT t.location, f.hostname, f.port, f.username, f.passowrd, f.location as ftp_location FROM tr_bwt_serde_config t LEFT JOIN tr_bwt_ftp_config f ON t.ftp_id = f.id AND t.id = ?", 1)
    ptmt.setString(1, tableId)
    val rs = ptmt.executeQuery()
    if (!rs.next()) {
      sys.exit(-1)
    }
    val hostname = rs.getString("hostname")
    val port = rs.getString("port")
    val username = rs.getString("username")
    val password = rs.getString("password")
    val ftp_location = rs.getString("ftp_location")
    val location = rs.getString("location")
    val ftpUri = s"""ftp://${username}:${password}@${hostname}:$port${ftp_location}/"""

    val conf = new Configuration()
    val path = new Path(location)
    val fs = path.getFileSystem(conf)
    fs.listStatus(path, new GlobFilter("*.DATA.gz")).map(_.getPath).map(src => (src, s"""${ftpUri}/${src.getName}""")).foreach {
      case (srcPath, target) => move(fs, srcPath, target)
    }
    fs.listStatus(path, new GlobFilter("*.VAL")).map(_.getPath).map(src => (src, s"""${ftpUri}/${src.getName}""")).foreach {
      case (srcPath, target) => move(fs, srcPath, target)
    }
    fs.listStatus(path, new GlobFilter("*.CHECK")).map(_.getPath).map(src => (src, s"""${ftpUri}/${src.getName}""")).foreach {
      case (srcPath, target) => move(fs, srcPath, target)
    }
  }
  def move(fs: FileSystem, src: Path, target: String): Unit = {
    val targetPath = new Path(target)
  }
}
