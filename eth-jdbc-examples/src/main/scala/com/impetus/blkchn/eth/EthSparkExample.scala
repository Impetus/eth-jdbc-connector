package com.impetus.blkchn.eth

import java.math.BigInteger

import com.impetus.blkch.spark.connector.rdd.ReadConf
import org.apache.spark.sql.eth.EthSpark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.Column

object EthSparkExample {
  import org.apache.spark.sql.eth.EthSpark.implicits._

  lazy val spark = SparkSession.builder().master("local").appName("Test").getOrCreate()

  def main(args : Array[String]): Unit = {
    testA1
    test1
    test2
    test3
    test4
  }
  def test1(): Unit ={
    Class.forName("com.impetus.eth.jdbc.EthDriver")
    val readConf = ReadConf(None, Some(30000), "Select * FROM block where blocknumber = 1")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf,
      Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545"))
    rdd.map(x => x.get(1)).collect().foreach(println)
    rdd.map(x => println(x.schema))
  }

  def testA1(): Unit ={
    Class.forName("com.impetus.eth.jdbc.EthDriver")
    val readConf = ReadConf(None, Some(30000), "select * from block where blocknumber > 1652349 and blocknumber < 1652354 and hash = '0x932fb58356934692f5167e4ccf29f01ba2cb3cc4bb1889a1a0a33bad79c6befc'")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf,
      Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545"))
    rdd.map(x => x.get(1)).collect().foreach(println)
    rdd.map(x => println(x.schema))
  }

  def test2(): Unit ={
    Class.forName("com.impetus.eth.jdbc.EthDriver")
    val readConf = ReadConf(Some(3), None, "Select * FROM block where blocknumber > 123 and blocknumber < 150")
    val rdd = EthSpark.load[Row](spark.sparkContext, readConf,
      Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545"))
    rdd.map(x => (x.get(1),x.get(2))).collect().foreach(println)
    rdd.collect().map(x => println(x.schema))
  }

  def test3: Unit = {
    Class.forName("com.impetus.eth.jdbc.EthDriver")
    val readConf = ReadConf(Some(4), None, "Select * FROM transaction where blocknumber > 1 and blocknumber < 145")
    val options = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545")//, "spark.blkchn.partitioner"->"com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner")
    val df = spark.read.format("org.apache.spark.sql.eth").options(options).
      load()
    val df2 = df.select(
      df.col("blocknumber").toString()
    )

    def bool(row:Row) = new BigInteger(row.getAs("blocknumber").toString()).compareTo(new BigInteger("12")) < 0
    df2.filter(x => bool(x)).show(false)

    df2.createOrReplaceTempView("block")
    println(df.schema)
    spark.sql("select blocknumber from block where blocknumber > 5").show(false)
  }

  def test4 = {
    Class.forName("com.impetus.eth.jdbc.EthDriver")
    val readConf = ReadConf(Some(20),None,"Select * from block where blocknumber > 123 and blocknumber < 150")
    val option = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545")
    val df = spark.read.format("org.apache.spark.sql.eth").options(option).load()
    df.show()
    println(df.schema)

  }
}