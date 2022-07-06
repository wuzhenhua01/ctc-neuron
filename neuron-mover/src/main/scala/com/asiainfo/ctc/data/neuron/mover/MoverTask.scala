package com.asiainfo.ctc.data.neuron.mover

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import org.apache.hadoop.conf.Configuration
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
    var rs: ResultSet = _
    var stmt: PreparedStatement = _
    var conn: Connection = _

    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/dataos_alarm?useSSL=false", "root", "root")
    try {
      val ptmt = conn.prepareStatement("SELECT * FROM tr_bwt_serde_config WHERE id = ?")
      try {
        ptmt.setString(1, tableId)
        val rs = ptmt.executeQuery()
        if (rs.next()) {
          println(rs.getString(1))
          println(rs.getString(2))
          println(rs.getString(3))
        }
      } finally {
        ptmt.close()
      }
    } finally {
      conn.close()
    }

    val conf = new Configuration()
  }
}
