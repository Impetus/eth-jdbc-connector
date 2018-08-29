package com.impetus.eth.test

import com.impetus.eth.query.EthColumns
import com.impetus.test.catagory.IntegrationTest
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.JdbcDialects
import com.impetus.eth.spark.sql.JdbcDielect.EthereumDialect

@IntegrationTest
class TestEthSparkJDBCTrxs extends FlatSpec with BeforeAndAfterAll with SharedSparkSession{

  var df:DataFrame = null

  override def beforeAll() {
    super.beforeAll()

    JdbcDialects.registerDialect(EthereumDialect)
    df = spark.read
      .format("jdbc")
      .option("driver", "com.impetus.eth.jdbc.EthDriver")
      .option("url", "jdbc:blkchn:ethereum://ropsten.infura.io/1234")
      .option("dbtable", "transaction")
      .load()
  }


  "Eth Spark JDBC Data Frame Transaction" should "have rows" in {
    val newDF=df.where("blocknumber > 2256446 and blocknumber < 2256451").select("blocknumber")
    newDF.show()
    assert(newDF.collect().length > 0)
  }

  it should "return empty data frame" in {
    val newDF = df
      .where("blocknumber > 123 and blocknumber < 145")
      .where("hash='2f3202c2d498c926401f6e74406525c02f735feefe457c5689'")
      .select("blocknumber", "hash")
    assert(newDF.count() === 0)
  }

  it should "return empty data frame with single where" in {
    val newDF = df
      .where("blocknumber > 123 and blocknumber < 145 and hash='2f3202c2d498c926401f6e74406525c02f735feefe457c5689'")
      .select("blocknumber", "hash")
    assert(newDF.count() === 0)
  }

  it should "return expected value with direct API And Schema" in {
    val newDF = df.where("hash = '0x525bd3ec5c5ed2d222ae04ed2a6cc00267d98e206f604e979000ffbc5f99cb4b'")
      .select(df.col("*"))
    newDF.cache()
    assert(newDF.count() === 1)
    val schema = newDF.schema.fields
    assertResult(EthColumns.BLOCKHASH)(schema(0).name)
    assertResult(EthColumns.BLOCKNUMBER)(schema(1).name)
    assertResult(EthColumns.CREATES)(schema(2).name)
    assertResult(EthColumns.FROM)(schema(3).name)
    assertResult(EthColumns.GAS)(schema(4).name)
    assertResult(EthColumns.GASPRICE)(schema(5).name)
    assertResult(EthColumns.HASH)(schema(6).name)
    assertResult(EthColumns.INPUT)(schema(7).name)
    assertResult(EthColumns.NONCE)(schema(8).name)
    assertResult(EthColumns.PUBLICKEY)(schema(9).name)
    assertResult(EthColumns.R)(schema(10).name)
    assertResult(EthColumns.RAW)(schema(11).name)
    assertResult(EthColumns.S)(schema(12).name)
    assertResult(EthColumns.TO)(schema(13).name)
    assertResult(EthColumns.TRANSACTIONINDEX)(schema(14).name)
    assertResult(EthColumns.V)(schema(15).name)
    assertResult(EthColumns.VALUE)(schema(16).name)
    newDF.unpersist()
  }
}
