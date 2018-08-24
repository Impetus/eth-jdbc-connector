/******************************************************************************* 
 * * Copyright 2018 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package org.apache.spark.sql.eth

import com.impetus.blkch.spark.connector.rdd.ReadConf
import com.impetus.blkch.spark.connector.util.Logging
import org.apache.spark.sql.{ Row, SQLContext }
import org.apache.spark.sql.sources.{ BaseRelation, RelationProvider, SchemaRelationProvider }
import org.apache.spark.sql.blkch.BlkchnSourceRelation
import org.apache.spark.sql.types.StructType

class DefaultSource extends RelationProvider with SchemaRelationProvider with Logging {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val readConf = ReadConf(sqlContext.sparkContext.conf, parameters)
    val readConfOptions = readConf.asOptions()
    val options = for ((key, value) <- parameters; if !readConfOptions.contains(key)) yield {
      (key, value)
    }
    val rdd = EthSpark.load[Row](sqlContext.sparkContext, readConf, options)
    val schema = rdd.getSchema()
    BlkchnSourceRelation(rdd, schema)(sqlContext)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val readConf = ReadConf(sqlContext.sparkContext.conf, parameters)
    val rdd = EthSpark.load[Row](sqlContext.sparkContext, readConf)
    val _schema = if (schema == null) rdd.first.schema else schema
    BlkchnSourceRelation(rdd, _schema)(sqlContext)
  }
}
