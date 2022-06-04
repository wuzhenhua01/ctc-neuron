package com.asiainfo.ctc.data.neuron

import java.util.Properties

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
class NeuronTableMetaClient {

}

object NeuronTableMetaClient {
  def withPropertyBuilder = new PropertyBuilder

  private val others = new Properties

  class PropertyBuilder private[NeuronTableMetaClient] {
    private var databaseName: String = _
    private var tableName: String = _

    def setDatabaseName(databaseName: String): PropertyBuilder = {
      this.databaseName = databaseName
      this
    }

    def setTableName(tableName: String): PropertyBuilder = {
      this.tableName = tableName
      this
    }
  }

}
