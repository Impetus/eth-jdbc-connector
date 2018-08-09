package com.impetus.eth.test

import com.impetus.test.catagory.UnitTest
import org.apache.spark.SparkConf
import org.scalatest.{BeforeAndAfter, FlatSpec}
import org.apache.spark.sql.eth._

@UnitTest
class TestEthConnectorConf extends FlatSpec with BeforeAndAfter {

  var ethConnectorConf:EthConnectorConf = null
  val conf = new SparkConf().set("placeHolder", "Temp")

  before {
    ethConnectorConf = new EthConnectorConf(conf, Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234",
      "KEYSTORE_PASSWORD" -> "impetus123", "KEYSTORE_PATH" -> "UTC--2017-09-11T04-53-29.614189140Z--8144c67b144a408abc989728e32965edf37adaa1"))
  }

  "EthConnectorConf" should "have default value" in {
    assert(EthConnectorConf.connectionURL.default.nonEmpty)
    assert(EthConnectorConf.keystorePassword.default.isEmpty)
    assert(EthConnectorConf.keystorePath.default.isEmpty)
  }

  it should "able to return passed value from option" in {
    assert(ethConnectorConf.connectionURL.equals("jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    assert(ethConnectorConf.keystorePath.equals("UTC--2017-09-11T04-53-29.614189140Z--8144c67b144a408abc989728e32965edf37adaa1"))
    assert(ethConnectorConf.keystorePassword.equals("impetus123"))
  }

  it should "able to return toString" in {
    assert(ethConnectorConf.toString.contains("impetus123"))
  }

  it should "able to connect to Eth BlockChain" in {
    val blkchnConnection = ethConnectorConf.getConnection()
    assert(!blkchnConnection.isClosed)
  }

  it should "able to work with spark conf only" in {
    val sparkConf = new SparkConf().set("url", "jdbc:blkchn:ethereum://ropsten.infura.io/1234").
      set("KEYSTORE_PASSWORD", "impetus123").set("KEYSTORE_PATH", "UTC--2017-09-11T04-53-29.614189140Z--8144c67b144a408abc989728e32965edf37adaa1")
    ethConnectorConf = EthConnectorConf(sparkConf)
    assert(ethConnectorConf.connectionURL.equals("jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    assert(ethConnectorConf.keystorePath.equals("UTC--2017-09-11T04-53-29.614189140Z--8144c67b144a408abc989728e32965edf37adaa1"))
    assert(ethConnectorConf.keystorePassword.equals("impetus123"))
    assert(ethConnectorConf.toString.contains("impetus123"))
  }

}
