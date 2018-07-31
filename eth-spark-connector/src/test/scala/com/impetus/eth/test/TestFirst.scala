package com.impetus.eth.test

import com.impetus.blkch.spark.connector.rdd.{BlkchnRDD, ReadConf}
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.test.catagory.IntegrationTest
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.eth.EthSpark
import org.scalatest.{BeforeAndAfter, FlatSpec}

class TestFirst extends FlatSpec with BeforeAndAfter with IntegrationTest{

  var spark: SparkSession = null
  val ethPartitioner:BlkchnPartitioner = TestEthPartitioner
  var readConf:ReadConf = null
  var rdd: BlkchnRDD[Row] = null

  before {
    spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    readConf = ReadConf(Some(3), None, "Select * FROM block")(ethPartitioner)
    rdd = EthSpark.load[Row](spark.sparkContext, readConf,Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    Class.forName("com.impetus.eth.jdbc.EthDriver")
  }

  "Eth Spark" should "have Read Conf" in {
    assert(readConf != null)
  }

  it should "have rdd with same partition as specified" in {
    assert(rdd.getNumPartitions == 3)
  }

  it should "have not empty rdd" in{
    assert(rdd.collect().isEmpty != true)
  }

  it should "be able to create data frame" in {
    val option = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545")
    val df = spark.read.format("org.apache.spark.sql.eth").options(option).load()
    assert(df.select(df.col("blocknumber")).collect().length > 0)
  }

  after {
    spark.stop()
  }




}
