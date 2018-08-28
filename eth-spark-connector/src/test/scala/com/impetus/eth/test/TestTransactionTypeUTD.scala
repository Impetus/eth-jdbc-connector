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
package com.impetus.eth.test

import com.impetus.blkch.spark.connector.rdd.{BlkchnRDD, ReadConf}
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.eth.jdbc.EthResultSetMetaData
import com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner
import com.impetus.test.catagory.IntegrationTest
import org.apache.spark.sql.eth.EthSpark
import org.apache.spark.sql.{Row}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec}
import org.apache.spark.sql.types.{ArrayType, StringType, TransactionType}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import com.impetus.blkch.spark.connector.rdd._

@IntegrationTest
class TestTransactionTypeUTD extends FlatSpec with BeforeAndAfterAll with SharedSparkSession {

  val ethPartitioner: BlkchnPartitioner = DefaultEthPartitioner
  var readConf: ReadConf = null
  var rdd: BlkchnRDD[Row] = null
  var transactions: Array[Any] = null
  var ethRDD: EthRDD[_] = null

  override def beforeAll() {
    super.beforeAll()
    readConf = ReadConf(Some(3), None, "Select transactions FROM block where blocknumber = 3796441")(ethPartitioner)
    rdd = EthSpark.load[Row](spark.sparkContext, readConf, Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234")).cache()
    ethRDD = new com.impetus.blkch.spark.connector.rdd.EthRDD(spark.sparkContext, null, null)
    transactions = rdd.map { row => row.get(0) }.collect()
  }

  "Transaction" should "give data in Transaction UTD" in {
    transactions = rdd.map { row => row.get(0) }.collect()
    assert(transactions(0).asInstanceOf[ArrayBuffer[_]].forall(_.isInstanceOf[TransactionType]))
  }

  it should "give data as expected" in {
    assert(transactions(0).asInstanceOf[ArrayBuffer[_]].forall(_.asInstanceOf[TransactionType].blockNumber.equals("3796441")))
    assert(transactions(0).asInstanceOf[ArrayBuffer[_]].forall(_.asInstanceOf[TransactionType].blockHash.equals("0x388f5e0c424dc5eda1bf8fa741cecd88e2082df3e3e4491e4565bce9bdb0b880")))
  }

  "EthRDD" should "give expected result with handleExtraType" in {
    val data = new java.util.ArrayList[String]
    val resultSetMetaData = new EthResultSetMetaData("block", Map("blocknumber" -> 0.asInstanceOf[Integer]).asJava,
      Map("blocknumber" -> "blocknumber").asJava, Map("blocknumber" -> (-5).asInstanceOf[Integer]).asJava,Map(0.asInstanceOf[Integer] -> "blocknumber").asJava)
    data.add("3124")
    val structType = ethRDD.handleExtraType(1, resultSetMetaData, data)
    assert(structType.name.equals("blocknumber"))
    assert(structType.dataType.equals(ArrayType(StringType, true)))
    val returnData = ethRDD.handleExtraData(1, resultSetMetaData, data)
    assert(returnData.equals(data.asScala))
  }

}
