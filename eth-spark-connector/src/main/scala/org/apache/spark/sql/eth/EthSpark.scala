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

import com.impetus.blkch.BlkchnException
import com.impetus.blkch.spark.connector.BlkchnConnector
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.blkch.spark.connector.rdd.{BlkchnRDD, EthRDD, ReadConf}
import com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.reflect.ClassTag

case class EthSpark(sparkSession: SparkSession, connector: BlkchnConnector, readConf: ReadConf) {

  private def rdd[D: ClassTag]: BlkchnRDD[D] = {
    new EthRDD[D](sparkSession.sparkContext, sparkSession.sparkContext.broadcast(connector), readConf)
  }

  def toRDD[D: ClassTag]: BlkchnRDD[D] = rdd[D]

  def toDF(): DataFrame = {
    sparkSession.read.format(EthFormat)
      .options(readConf.asOptions()).load()
  }

  def save(dataFrame: DataFrame) ={
    def createInsertStat(row: Row): String = {
      val sb = new StringBuilder
      sb.append("insert into transaction (toAddress, value, unit, async) values (")
      sb.append("'" + (if(row.get(0) == null) "0x0" else row.get(0).toString) + "',")
      sb.append(if(row.get(1) == null) "0" else row.get(1).toString)
      sb.append(",'" + (if(row.get(2) == null) "ether" else row.get(2).toString) + "',")
      sb.append(if(row.get(3) == null) "false" else row.get(3).toString)
      sb.append(s")")
      sb.toString
    }
    if(dataFrame.schema.zipWithIndex.size < 4)
      throw new BlkchnException("Dataframe to save should have four column in sequence of (toaddress, value, unit, async)")
    else{
      dataFrame.schema.zipWithIndex.foreach{
        case (structType, index) =>
          if(index == 0) require(structType.name == "toaddress" && structType.dataType.typeName == "string",
            s"column name or type not matched (${structType.name},${structType.dataType.typeName}), should be (toaddress, String)")
          else if(index == 1) require(structType.name == "value",
            s"column name not matched (${structType.name}), should be (value)")
          else if(index == 2) require(structType.name == "unit" && structType.dataType.typeName == "string",
            s"column name or type not matched (${structType.name},${structType.dataType.typeName}), should be (unit, String)")
          else if(index == 3) require(structType.name == "async" && structType.dataType.typeName == "string",
            s"column name or type not matched (${structType.name},${structType.dataType.typeName}), should be (async, String)")
      }
    }
    dataFrame.foreachPartition {
      rows =>
        connector.withStatementDo {
          stat =>
            for(row <- rows) {
              val insertCmd = createInsertStat(row)
              stat.execute(insertCmd)
            }
        }
    }
  }

}

object EthSpark {

  def builder(): Builder = new Builder

  def load[D: ClassTag](sc: SparkContext): BlkchnRDD[D] = load(sc, ReadConf(sc.conf))

  def load[D: ClassTag](sc: SparkContext, readConf: ReadConf): BlkchnRDD[D] = load(sc, readConf, Map())

  def load[D: ClassTag](sc: SparkContext, readConf: ReadConf, options: Map[String, String]): BlkchnRDD[D] = {
    builder().sc(sc).readConf(readConf).options(options).build().toRDD
  }

  def insertTransaction(readConf: ReadConf, options: Map[String, String]) = {
    val ethCon = new BlkchnConnector(EthConnectorConf(new SparkConf(), options ++ readConf.asOptions()))
    val transactionStatus = ethCon.withStatementDo(stat => stat.execute(readConf.query))
    transactionStatus
  }

  def load(spark: SparkSession): DataFrame = builder().sparkSession(spark).build().toDF()

  def save(dataframe: DataFrame, options: Map[String, String])= {
    builder().sparkSession(dataframe.sparkSession).options(options).mode(Mode.Write).build().save(dataframe)
  }

  class Builder {

    private var sparkSession: Option[SparkSession] = None
    private var connector: Option[BlkchnConnector] = None
    private var readConfig: Option[ReadConf] = None
    private var options: Map[String, String] = Map()
    private var mode: Mode.Value = Mode.Read

    def sparkSession(sparkSession: SparkSession): Builder = {
      this.sparkSession = Some(sparkSession)
      this
    }

    def sc(sc: SparkContext): Builder = {
      this.sparkSession = Some(SparkSession.builder().config(sc.getConf).getOrCreate())
      this
    }

    def connector(connector: BlkchnConnector): Builder = {
      this.connector = Some(connector)
      this
    }

    def readConf(readConf: ReadConf): Builder = {
      this.readConfig = Some(readConf)
      this
    }

    def option(key: String, value: String): Builder = {
      this.options = this.options + (key -> value)
      this
    }

    def options(options: Map[String, String]): Builder = {
      this.options = options
      this
    }

    def mode(mode: Mode.Value): Builder = {
      this.mode = mode
      this
    }

    def build(): EthSpark = {
      require(sparkSession.isDefined, "The SparkSession must be set, either explicitly or via the SparkContext")
      val session = sparkSession.get
      val readConf = readConfig match {
        case Some(config) => config
        case None => mode match {
          case Mode.Read => ReadConf(session.sparkContext.conf, options)
          case Mode.Write => null
        }
      }
      val conn = connector match {
        case Some(connect) => connect
        case None => new BlkchnConnector(EthConnectorConf(session.sparkContext.conf, options))
      }
      new EthSpark(session, conn, readConf)
    }
  }

  private object Mode extends Enumeration {
    type Mode = Value

    val Read = Value
    val Write = Value
  }

  object implicits extends Serializable {
    implicit def getEthPartitioner: BlkchnPartitioner = DefaultEthPartitioner
  }
}
