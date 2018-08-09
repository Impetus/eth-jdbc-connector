package com.impetus.eth.test

import java.math.BigInteger

import com.impetus.blkch.spark.connector.rdd.ReadConf
import com.impetus.blkch.spark.connector.BlkchnConnector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.eth.EthConnectorConf
import org.scalatest.{BeforeAndAfter, FlatSpec}
import com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner
import com.impetus.test.catagory.{UnitTest}
import org.junit.experimental.categories.Category

@Category(Array(classOf[UnitTest]))
class TestDefaultEthPartitioner extends FlatSpec with BeforeAndAfter with UnitTest {

  var spark: SparkSession = null
  var ethConnectorConf : EthConnectorConf = null
  var blkchnConnector : BlkchnConnector = null
  var defaultPartition : DefaultEthPartitioner = null

  before {
    spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    Class.forName("com.impetus.eth.jdbc.EthDriver")
    ethConnectorConf = EthConnectorConf(spark.sparkContext.getConf,Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    blkchnConnector = new BlkchnConnector(ethConnectorConf)
    defaultPartition = DefaultEthPartitioner
  }

  "Default Eth Partition" should "give same number of partition as specified" in {
    val readConf = ReadConf(Some(3), None, "Select * FROM block")(defaultPartition)
    val bufferOfRange = defaultPartition.getPartitions(blkchnConnector,readConf)
    assert(bufferOfRange.size == 3)
  }

  it should "give same number of line in partition if specified fetchSizeInRows" in {
    val readConf = ReadConf(None, Some(300000), "Select * FROM block")(defaultPartition)
    val bufferOfRange = defaultPartition.getPartitions(blkchnConnector,readConf)
    val max = bufferOfRange(0).range.getRangeList.getRanges.get(0).getMax.asInstanceOf[BigInteger]
    val min = bufferOfRange(0).range.getRangeList.getRanges.get(0).getMin.asInstanceOf[BigInteger]
    assert(max.subtract(min).intValue() + 1 == 300000)
  }

}
