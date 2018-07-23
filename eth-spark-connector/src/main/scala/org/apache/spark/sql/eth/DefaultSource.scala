package org.apache.spark.sql.eth

import com.impetus.blkch.spark.connector.BlkchnConnector
import com.impetus.blkch.spark.connector.rdd.ReadConf
import com.impetus.blkch.spark.connector.util.Logging
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.blkch.BlkchnSourceRelation
import org.apache.spark.sql.types.StructType
import java.util.logging.Logging

class DefaultSource extends RelationProvider with SchemaRelationProvider with Logging {



  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val readConf = ReadConf(sqlContext.sparkContext.conf, parameters)
    val readConfOptions = readConf.asOptions()
    val options = for((key, value) <- parameters; if(!readConfOptions.contains(key))) yield {
      (key, value)
    }
    val rdd = EthSpark.load[Row](sqlContext.sparkContext, readConf, options)
    val schema = rdd.first.schema
    BlkchnSourceRelation(rdd, schema)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val readConf = ReadConf(sqlContext.sparkContext.conf, parameters)
    val rdd = EthSpark.load[Row](sqlContext.sparkContext, readConf)
    val _schema = if(schema == null) rdd.first.schema else schema
    BlkchnSourceRelation(rdd, _schema)(sqlContext)
  }
}
