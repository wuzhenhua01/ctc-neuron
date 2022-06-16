package com.asiainfo.ctc.data.neuron.jdbc

import org.apache.log4j.Logger

case class JDBCConnector() {

}

object JDBCConnector {
  private lazy val LOG = Logger.getLogger(getClass)

  private val Database: String = "database"
  private val TableName: String = "tablename"
  private val Where: String = "where"
  private val Url: String = "url"
  private val User: String = "user"
  private val Password: String = "password"
  private val Driver: String = "driver"

  private val DefaultDriver = "com.mysql.jdbc.Driver"
  private val DefaultDatabase = "default"
  private val EmptyString = ""

  private def isJDBCDriverLoaded(driver: String): Boolean = {
    try {
      Class.forName(driver, false, this.getClass.getClassLoader)
      true
    } catch {
      case e: ClassNotFoundException =>
        LOG.error(s"JDBC driver $driver provided is not found in class path", e)
        false
    }
  }
}
