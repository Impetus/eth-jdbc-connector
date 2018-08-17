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
package com.impetus.blkchn.eth

import java.math.BigInteger

import com.impetus.blkch.spark.connector.rdd.ReadConf
import org.apache.spark.sql.eth.EthSpark
import org.apache.spark.sql.{ Row, SparkSession }
import org.apache.spark.sql.types.TransactionType
import scala.collection.mutable.ArrayBuffer
import org.slf4j.LoggerFactory

object EthSparkExample {
  import org.apache.spark.sql.eth.EthSpark.implicits._

  private val LOGGER = LoggerFactory.getLogger("com.impetus.blkchn.eth.EthSparkExample")

  lazy val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()

  def main(args: Array[String]): Unit = {
    test31
    testA1
    test1
    test2
    test3
    test4
    testInsert
    testDataFrameSave
  }
  def test1 = {
    val readConf = ReadConf(None, Some(30000), "select * from block where blocknumber > 123 and blocknumber < 132 and hash='2f32268b02c2d498c926401f6e74406525c02f735feefe457c5689'")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf,
      Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    LOGGER.info(rdd.collect().size.toString)
    rdd.map(x => x.get(1)).collect().foreach(x => LOGGER.info(x.toString))
    rdd.foreach(x => LOGGER.info(x.schema.toString()))
  }

  def test21 = {
    val readConf = ReadConf(Some(4), None, "Select transactions FROM block where blocknumber = 3796441")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf,
      Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    rdd.collect().foreach(x => LOGGER.info(x.toString))
    val transactions = rdd.map { row => row.get(0) }.collect()
    assert(transactions(0).asInstanceOf[ArrayBuffer[_]].forall(_.isInstanceOf[TransactionType]))
  }

  def testA1 = {
    val readConf = ReadConf(None, Some(30000), "select * from block where hash = '0x932fb58356934692f5167e4ccf29f01ba2cb3cc4bb1889a1a0a33bad79c6befc'")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf,
      Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    rdd.map(x => x.get(1)).collect().foreach(x => LOGGER.info(x.toString))
    rdd.foreach(x => LOGGER.info(x.schema.toString()))
  }

  def test2 = {
    val readConf = ReadConf(Some(3), None, "Select * FROM block where blocknumber > 123 and blocknumber < 150")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf,
      Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    rdd.map(x => (x.get(1), x.get(2))).collect().foreach(x => LOGGER.info(x.toString))
    rdd.collect().foreach(x => LOGGER.info(x.schema.toString()))
  }

  def test31 = {
    val blockhash = "0x938ecd8eee580ac0998571d4105cb27266926a24686a66dbcc36cf609ece5f05"
    val readConf = ReadConf(Some(4), None, s"Select * FROM transaction where blockhash = '$blockhash'")
    val options = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234") // "spark.blkchn.partitioner" -> "com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner"
    val df = spark.read.format("org.apache.spark.sql.eth").options(options).
      load()
    df.show()
  }

  def test3 = {
    val readConf = ReadConf(Some(4), None, "Select * FROM transaction where blocknumber > 1 and blocknumber < 145")
    val options = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234") // "spark.blkchn.partitioner" -> "com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner"
    val df = spark.read.format("org.apache.spark.sql.eth").options(options).
      load()
    val df1 = df.select(
      df.col("blockhash").toString())
    LOGGER.info(df1.collect()(0).get(0).toString)
    val df2 = df.select(
      df.col("blocknumber").toString())

    def bool(row: Row) = new BigInteger(row.getAs("blocknumber").toString).compareTo(new BigInteger("12")) < 0
    df2.filter(x => bool(x)).show(false)

    df2.createOrReplaceTempView("block")
    LOGGER.info(df2.schema.toString())
    spark.sql("select blocknumber from block where blocknumber > 5").show(false)
  }

  def test4 = {
    val readConf = ReadConf(Some(20), None, "Select * from block where blocknumber > 123 and blocknumber < 150")
    val option = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234")
    val df = spark.read.format("org.apache.spark.sql.eth").options(option).load()
    df.show()
    LOGGER.info(df.schema.toString())
  }

  def testInsert = {
    val readConf = ReadConf(None, None,
      "insert into transaction (toAddress, value, unit, async) values ('8144c67b144a408abc989728e32965edf37adaa1', 1.11, 'ether', false)")
    val transactionStatus = EthSpark.insertTransaction(readConf, Map(
      "url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234",
      "KEYSTORE_PATH" -> "<Path To Keystore>",
      "KEYSTORE_PASSWORD" -> "<password>"))
    LOGGER.info(s"\n\nInsert Transaction ${if (transactionStatus) "succeeded" else "failed"}")

  }

  def testDataFrameSave ={
    var output = spark.createDataFrame(Seq(
      ("8144c67b144a408abc989728e32965edf37adaa1", 2),
      ("8144c67b144a408abc989728e32965edf37adaa1", 2)
    )).toDF("address", "value_in_ether")
    val transactionStatus = EthSpark.save(output,Map(
      "url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234",
      "KEYSTORE_PATH" -> "<Path To Keystore>",
      "KEYSTORE_PASSWORD" -> "<password>"))
  }


}
