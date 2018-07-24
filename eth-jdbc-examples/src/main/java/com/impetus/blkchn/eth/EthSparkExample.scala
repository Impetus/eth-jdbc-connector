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
    //test2
    //test3
  }
  def test1(): Unit ={
    Class.forName("com.impetus.eth.jdbc.EthDriver")
    val readConf = ReadConf(None, Some(3), "Select * FROM block")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf,
      Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545"))
    rdd.collect().foreach(println)
    rdd.collect().map(x => println(x.schema))
  }

  def test2(): Unit ={
    Class.forName("com.impetus.eth.jdbc.EthDriver")
    val readConf = ReadConf(Some(3), None, "Select * FROM block")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf,
      Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545"))
    rdd.collect().foreach(println)
    rdd.collect().map(x => println(x.schema))
  }

  def test3: Unit = {
    Class.forName("com.impetus.eth.jdbc.EthDriver")
    val readConf = ReadConf(Some(4), None, "Select * FROM block")
    val options = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545")//, "spark.blkchn.partitioner"->"com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner")
    val df = spark.read.format("org.apache.spark.sql.eth").options(options).
      load()
    df.show
    df.createOrReplaceTempView("block")
    spark.sql("select blocknumber from block where blocknumber like '%12%'").show(false)
  }
}
