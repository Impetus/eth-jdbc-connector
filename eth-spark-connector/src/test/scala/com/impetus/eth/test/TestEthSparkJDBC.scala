/******************************************************************************* 
 * * Copyright 2018 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package com.impetus.eth.test

import com.impetus.test.catagory.IntegrationTest
import org.scalatest.FlatSpec
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.jdbc.JdbcDialects
import com.impetus.eth.spark.sql.JdbcDielect.EthereumDialect

@IntegrationTest
class TestEthSparkJDBC extends FlatSpec with BeforeAndAfterAll with SharedSparkSession {

  var df: DataFrame = null

  override def beforeAll() {
    super.beforeAll()
    JdbcDialects.registerDialect(EthereumDialect)
    df = spark.read
      .format("jdbc")
      .option("driver", "com.impetus.eth.jdbc.EthDriver")
      .option("url", "jdbc:blkchn:ethereum://ropsten.infura.io/1234")
      .option("dbtable", "block").load()
  }

  "Eth Spark JDBC Data Frame Block" should "have rows" in {
    val newDF = df.where("blocknumber > 2256446 and blocknumber < 2256451").select("blocknumber", "hash", "transactions")
    newDF.show()
    assert(newDF.collect().length > 0)
    assert(newDF.collect().length == 4)
  }

  it should " get all block columns" in {
    val newDF = df.where("blocknumber > 2256446 and blocknumber < 2256451")
    newDF.cache()
    newDF.show()
    assert(newDF.collect().length > 0)
    assert(newDF.collect().length == 4)
    assert(newDF.first().size == 21)
    newDF.unpersist()
  }

  it should "return empty data frame" in {
    val newDF = df
                .where("blocknumber > 123 and blocknumber < 145")
                .where("hash='2f32268b02c2d498c926401f6e74406525c02f735feefe457c5689'")
                .select("blocknumber", "hash", "transactions")
    assert(newDF.count() === 0)
  }

  it should "return expected value with direct API And Schema" in {
    val newDF = df.where("hash = '0xa9434bdf814018828d29c40577325daa3b9f4a345ecd4deadc11087d3dc9e829'")
      .select(df.col("*"))
    newDF.cache()
    assert(newDF.count() === 1)
    val schema = newDF.schema.fields
    assertResult("blocknumber")(schema(0).name)
    assertResult("hash")(schema(1).name)
    assertResult("parenthash")(schema(2).name)
    assertResult("nonce")(schema(3).name)
    assertResult("sha3uncles")(schema(4).name)
    assertResult("logsbloom")(schema(5).name)
    assertResult("transactionsroot")(schema(6).name)
    assertResult("stateroot")(schema(7).name)
    assertResult("receiptsroot")(schema(8).name)
    assertResult("author")(schema(9).name)
    assertResult("miner")(schema(10).name)
    assertResult("mixhash")(schema(11).name)
    assertResult("totaldifficulty")(schema(12).name)
    assertResult("extradata")(schema(13).name)
    assertResult("size")(schema(14).name)
    assertResult("gaslimit")(schema(15).name)
    assertResult("gasused")(schema(16).name)
    assertResult("timestamp")(schema(17).name)
    assertResult("transactions")(schema(18).name)
    assertResult("uncles")(schema(19).name)
    assertResult("sealfields")(schema(20).name)

    assertResult("array")(schema(18).dataType.typeName)
    assertResult("decimal(38,0)")(schema(0).dataType.typeName)
    assertResult("string")(schema(1).dataType.typeName)

    newDF.unpersist()
  }

}