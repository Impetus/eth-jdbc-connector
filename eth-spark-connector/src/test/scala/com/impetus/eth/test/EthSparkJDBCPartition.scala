package com.impetus.eth.test

import com.impetus.eth.spark.sql.JdbcDielect.EthereumDialect
import com.impetus.test.catagory.IntegrationTest
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.JdbcDialects
import org.scalatest.{BeforeAndAfterAll, FlatSpec}
import org.apache.spark.sql.functions.{min, max}

@IntegrationTest
class EthSparkJDBCPartition extends FlatSpec with BeforeAndAfterAll with SharedSparkSession{

  var df:DataFrame = null

  override def beforeAll() {
    super.beforeAll()

    JdbcDialects.registerDialect(EthereumDialect)
    df = spark.read
      .format("jdbc")
      .option("driver", "com.impetus.eth.jdbc.EthDriver")
      .option("url", "jdbc:blkchn:ethereum://ropsten.infura.io/1234")
      .option("partitionColumn", "blocknumber")
      .option("lowerBound", "2256400")
      .option("upperBound", "2256500")
      .option("numPartitions", "4")
      .option("dbtable", "block")
      .load()
  }

  "Eth Spark JDBC Data Frame" should "have same partion as mention" in {
    assert(df.rdd.getNumPartitions === 4)
  }

  it should "have data as expected" in {
    val newDF = df.select("blocknumber").where("blocknumber > 2256399 and blocknumber < 2256501").agg(min("blocknumber"), max("blocknumber")).cache()
    newDF.show()
    newDF.collect().map{
      row =>
        row.get(0).toString == "2256400"
        row.get(1).toString == "2257000"
    }
  }

  override def afterAll(): Unit = {
    df.unpersist()
  }

}
