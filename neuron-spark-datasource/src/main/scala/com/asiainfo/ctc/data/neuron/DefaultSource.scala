package com.asiainfo.ctc.data.neuron

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * @author wuzh8@asiainfo.com
 * @version 1.0.0
 * @since 2022-06-04
 */
class DefaultSource extends CreatableRelationProvider with DataSourceRegister {
  override def shortName(): String = "neuron"

  /**
   * This DataSource API is used for writing the DataFrame at the destination. For now, we are returning a dummy
   * relation here because Spark does not really make use of the relation returned, and just returns an empty
   * dataset at [[org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand.run()]]. This saves us the cost
   * of creating and returning a parquet relation here.
   *
   * @param sqlContext Spark SQL Context
   * @param mode Mode for saving the DataFrame at the destination
   * @param parameters Parameters passed as part of the DataFrame write operation
   * @param df Spark DataFrame to be written
   * @return Spark Relation
   */
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], df: DataFrame): BaseRelation = {
    NeuronSparkSqlWriter.write(sqlContext, mode, parameters, df)
    new NeuronEmptyRelation(sqlContext, df.schema)
  }
}
