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
import com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner
import com.impetus.test.catagory.IntegrationTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.eth.EthSpark
import org.scalatest.{ BeforeAndAfterAll, FlatSpec}

@IntegrationTest
class TestEthSpark extends FlatSpec with BeforeAndAfterAll with SharedSparkSession {

  val ethPartitioner: BlkchnPartitioner = DefaultEthPartitioner
  var readConf: ReadConf = null
  var rdd: BlkchnRDD[Row] = null

  override def beforeAll() {
    super.beforeAll()
    readConf = ReadConf(Some(3), None, "Select * FROM block where blocknumber > 5 and blocknumber < 30")(ethPartitioner)
    rdd = EthSpark.load[Row](spark.sparkContext, readConf, Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    rdd.cache()
  }

  "Eth Spark" should "have Read Conf" in {
    assert(readConf != null)
  }

  it should "have rdd with same partition as specified" in {
    assert(rdd.getNumPartitions == 3)
  }

  it should " not have empty rdd" in {
    assert(rdd.collect().isEmpty != true)
  }

  it should "be able to create data frame" in {
    val option = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234")
    val df = spark.read.format("org.apache.spark.sql.eth").options(option).load()
    assert(df.select(df.col("blocknumber")).collect().length > 0)
  }

  it should "be able to create block RDD" in {
    readConf = ReadConf(Some(3), None, "Select * FROM block where blocknumber > 12345 and blocknumber <12350")(ethPartitioner)
    rdd = EthSpark.load(spark.sparkContext, readConf, Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    val rddSchemaList = rdd.getSchema.fields
    assertResult(4)(rdd.count())
    assertResult("blocknumber")(rddSchemaList(0).name)
    assertResult("hash")(rddSchemaList(1).name)
    assertResult("parenthash")(rddSchemaList(2).name)
    assertResult("nonce")(rddSchemaList(3).name)
    assertResult("sha3uncles")(rddSchemaList(4).name)
    assertResult("logsbloom")(rddSchemaList(5).name)
    assertResult("transactionsroot")(rddSchemaList(6).name)
    assertResult("stateroot")(rddSchemaList(7).name)
    assertResult("receiptsroot")(rddSchemaList(8).name)
    assertResult("author")(rddSchemaList(9).name)
    assertResult("miner")(rddSchemaList(10).name)
    assertResult("mixhash")(rddSchemaList(11).name)
    assertResult("totaldifficulty")(rddSchemaList(12).name)
    assertResult("extradata")(rddSchemaList(13).name)
    assertResult("size")(rddSchemaList(14).name)
    assertResult("gaslimit")(rddSchemaList(15).name)
    assertResult("gasused")(rddSchemaList(16).name)
    assertResult("timestamp")(rddSchemaList(17).name)
    assertResult("transactions")(rddSchemaList(18).name)
    assertResult("uncles")(rddSchemaList(19).name)
    assertResult("sealfields")(rddSchemaList(20).name)

  }

  it should "be able to create transaction RDD" in {
    readConf = ReadConf(Some(3), None, "Select * FROM transaction where blocknumber > 2245600 and blocknumber <2245610")(ethPartitioner)
    rdd = EthSpark.load(spark.sparkContext, readConf, Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    val rddSchemaList = rdd.getSchema.fields
    assertResult(29)(rdd.count())
    assertResult("blockhash")(rddSchemaList(0).name)
    assertResult("blocknumber")(rddSchemaList(1).name)
    assertResult("creates")(rddSchemaList(2).name)
    assertResult("from")(rddSchemaList(3).name)
    assertResult("gas")(rddSchemaList(4).name)
    assertResult("gasprice")(rddSchemaList(5).name)
    assertResult("hash")(rddSchemaList(6).name)
    assertResult("input")(rddSchemaList(7).name)
    assertResult("nonce")(rddSchemaList(8).name)
    assertResult("publickey")(rddSchemaList(9).name)
    assertResult("r")(rddSchemaList(10).name)
    assertResult("raw")(rddSchemaList(11).name)
    assertResult("s")(rddSchemaList(12).name)
    assertResult("to")(rddSchemaList(13).name)
    assertResult("transactionindex")(rddSchemaList(14).name)
    assertResult("v")(rddSchemaList(15).name)
    assertResult("value")(rddSchemaList(16).name)
  }

  "Read Conf asOption" should "return Default Partition Name" in {
    val option = readConf.asOptions()
    assert(option.getOrElse("spark.blkchn.partitioner", "").toString.equals(ethPartitioner.getClass.getCanonicalName.replaceAll("\\$", "")))
  }
}