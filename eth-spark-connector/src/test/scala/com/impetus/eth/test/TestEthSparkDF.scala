package com.impetus.eth.test

import com.impetus.blkch.spark.connector.rdd.{BlkchnRDD, ReadConf}
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner
import com.impetus.test.catagory.IntegrationTest
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DecimalType, StringType, TransactionUTD}
import org.scalatest.{BeforeAndAfter, FlatSpec}

@IntegrationTest
class TestEthSparkDF extends FlatSpec with BeforeAndAfter{

  var spark: SparkSession = null
  val ethPartitioner:BlkchnPartitioner = DefaultEthPartitioner
  var readConf:ReadConf = null
  var df: DataFrame = null

  before {
    spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    readConf = ReadConf(Some(3), None, "Select blocknumber, hash, transactions FROM block where blocknumber > 1 and blocknumber < 30")(ethPartitioner)
  }

  "Eth Spark Data Frame" should "have rows" in {
    val option = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545")
    df = spark.read.format("org.apache.spark.sql.eth").options(option).load()
    df.cache()
    assert(df.collect().length > 0)
    assert(df.collect().length == 28)
  }

  it should "have expected number of Column" in {
    val valueMap = df.collect().map(_.size)
    assert(!valueMap.exists(_ != 3))
  }

  it should "have same column type as Expected" in {
    val valueMap = df.schema
    for(typeName <- valueMap){
     typeName.name match{
       case "blocknumber" => assert(typeName.dataType.equals(DecimalType(38,0)))
       case "hash" => assert(typeName.dataType.equals(StringType))
       case "transactions" => assert(typeName.dataType.equals(ArrayType(TransactionUTD,true)))
     }
    }
    spark.stop()
  }

}
