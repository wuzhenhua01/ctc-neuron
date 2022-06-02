package com.asiainfo.ctc.data.neuron

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DefaultSource extends CreatableRelationProvider with DataSourceRegister {
  override def shortName(): String = "neuron"

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], df: DataFrame): BaseRelation = {
    NeuronSparkSqlWriter.write(sqlContext, mode, parameters, df)
    new NeuronEmptyRelation(sqlContext, df.schema)
  }
}
