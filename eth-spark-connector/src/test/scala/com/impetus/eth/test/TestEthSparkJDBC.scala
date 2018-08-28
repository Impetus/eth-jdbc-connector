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

    JdbcDialects.registerDialect(EthereumDialect);
    df = spark.read
      .format("jdbc")
      .option("driver", "com.impetus.eth.jdbc.EthDriver")
      .option("url", "jdbc:blkchn:ethereum://ropsten.infura.io/1234")
      .option("dbtable", "block").load()
  }

  "Eth Spark JDBC Data Frame" should "have rows" in {
    val newDF = df.where("blocknumber > 2256446 and blocknumber < 2256451").select("blocknumber", "hash", "transactions")
    newDF.show()
    assert(newDF.collect().length > 0)
    assert(newDF.collect().length == 4)
  }

  it should " get all block columns" in {
    val newDF = df.where("blocknumber > 2256446 and blocknumber < 2256451")
    newDF.show()
    assert(newDF.collect().length > 0)
    assert(newDF.collect().length == 4)
  }
}