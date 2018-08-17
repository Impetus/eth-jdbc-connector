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

import com.impetus.blkch.spark.connector.rdd.ReadConf
import com.impetus.test.catagory.IntegrationTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.eth.EthSpark
import org.apache.spark.sql.types.TransactionType
import org.scalatest.FunSuite
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

@IntegrationTest
class TestEthSparkSamples extends FunSuite with SharedSparkSession {
  import org.apache.spark.sql.eth.EthSpark.implicits._

  private val LOGGER = LoggerFactory.getLogger(classOf[TestEthSparkSamples])
  private val url = "jdbc:blkchn:ethereum://ropsten.infura.io/1234"

  test("empty rdd") {
    val readConf = getReadConf(None, Some(30000), "select * from block where blocknumber > 123 and blocknumber < 132 and hash='2f32268b02c2d498c926401f6e74406525c02f735feefe457c5689'")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf, Map("url" -> url))
    assert(rdd.collect().size == 0)
  }

  test("transactions type with direct api") {
    val readConf = getReadConf(Some(4), None, "Select transactions FROM block where blocknumber = 3796441")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf, Map("url" -> url))
    val transactions = rdd.map { row => row.get(0) }.collect()
    assert(transactions(0).asInstanceOf[ArrayBuffer[_]].forall(_.isInstanceOf[TransactionType]))
  }

  test("with range node block table") {
    val readConf = getReadConf(Some(3), None, "Select * FROM block where blocknumber > 123 and blocknumber < 150")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf, Map("url" -> url))
    rdd.map(x => (x.get(1), x.get(2))).collect().foreach(x => LOGGER.info(x.toString))
    rdd.collect().foreach(x => LOGGER.info(x.schema.toString()))
  }

  test("with range node transaction table and spark sql") {
    val readConf = getReadConf(Some(4), None, "Select * FROM transaction where blocknumber > 1 and blocknumber < 50")
    val options = readConf.asOptions() ++ Map("url" -> url)
    val df = spark.read.format("org.apache.spark.sql.eth").options(options).
      load()
    val df2 = df.select(df.col("blocknumber"))
    df2.createOrReplaceTempView("block")
    LOGGER.info(df2.schema.toString())
    spark.sql("select blocknumber from block where blocknumber < 15").show(false)
  }

  test("data frame test") {
    val readConf = getReadConf(Some(20), None, "Select * from block where blocknumber > 123 and blocknumber < 150")
    val option = readConf.asOptions() ++ Map("url" -> url)
    val df = spark.read.format("org.apache.spark.sql.eth").options(option).load()
    df.show()
    LOGGER.info(df.schema.toString())
  }

  test("save dataFrame") {
    var output = spark.createDataFrame(Seq(
      ("8144c67b144a408abc989728e32965edf37adaa1", 1)
    )).toDF("address", "value_in_ether")
    val transactionStatus = EthSpark.save(output,Map(
      "url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234",
      "KEYSTORE_PATH" -> getClass.getResource("/UTC--2017-09-11T04-53-29.614189140Z--8144c67b144a408abc989728e32965edf37adaa1").getPath,
      "KEYSTORE_PASSWORD" -> "impetus123"))
  }

  def getReadConf(splitCount: Option[Int], fetchSizeInRows: Option[Int], query: String): ReadConf = {
    ReadConf(splitCount, fetchSizeInRows, query)
  }
}
