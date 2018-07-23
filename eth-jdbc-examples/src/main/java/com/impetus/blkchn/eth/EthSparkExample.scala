package com.impetus.blkchn.eth

import com.impetus.blkch.spark.connector.rdd.ReadConf
import com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.eth.EthSpark



object EthSparkExample {
  import org.apache.spark.sql.eth.EthSpark.implicits._

  lazy val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()

  def main(args : Array[String]): Unit = {
    test1
  }
  def test1(): Unit ={
    Class.forName("com.impetus.eth.jdbc.EthDriver")
    val readConf = ReadConf(Some(4), None, "Select * FROM block")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf,
      Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545"))
    rdd.collect().foreach(println)
    rdd.collect().map(x => println(x.schema))
  }

}
