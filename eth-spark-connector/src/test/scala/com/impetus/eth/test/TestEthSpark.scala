package com.impetus.eth.test

import com.impetus.blkch.spark.connector.rdd.{BlkchnRDD, ReadConf}
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner
import com.impetus.test.catagory.IntegrationTest
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.eth.EthSpark
import org.scalatest.{BeforeAndAfter, FlatSpec}

class TestEthSpark extends FlatSpec with BeforeAndAfter with IntegrationTest{

  var spark: SparkSession = null
  val ethPartitioner:BlkchnPartitioner = DefaultEthPartitioner
  var readConf:ReadConf = null
  var rdd: BlkchnRDD[Row] = null

  before {
    spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    readConf = ReadConf(Some(3), None, "Select * FROM block where blocknumber > 1 and blocknumber < 30")(ethPartitioner)
    rdd = EthSpark.load[Row](spark.sparkContext, readConf,Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    rdd.cache()
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

  "Read Conf asOption" should "return Default Partition Name" in {
    val option = readConf.asOptions()
    assert(option.getOrElse("spark.blkchn.partitioner","").toString.equals(ethPartitioner.getClass.getCanonicalName.replaceAll("\\$","")))
  }

  after {
    spark.stop()
  }

}
