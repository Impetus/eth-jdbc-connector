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

import com.impetus.blkch.spark.connector.rdd.{ ReadConf }
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner
import com.impetus.test.catagory.IntegrationTest
import org.apache.spark.sql.{ DataFrame }
import org.apache.spark.sql.types.{ArrayType, DecimalType, StringType, TransactionUTD}
import org.scalatest.{ BeforeAndAfterAll, FlatSpec}

@IntegrationTest
class TestEthSparkDF extends FlatSpec with BeforeAndAfterAll with SharedSparkSession {

  val ethPartitioner: BlkchnPartitioner = DefaultEthPartitioner
  var readConf: ReadConf = null
  var df: DataFrame = null

  override def beforeAll() {
    super.beforeAll()
    readConf = ReadConf(Some(3), None, "Select blocknumber,hash,transactions FROM block where blocknumber > 5 and blocknumber < 30")(ethPartitioner)
    val option = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234")
    df = spark.read.format("org.apache.spark.sql.eth").options(option).load().cache()
  }

  "Eth Spark Data Frame" should "have rows" in {
    assert(df.collect().length > 0)
    assert(df.collect().length == 24)
  }

  it should "have expected number of Column" in {
    val valueMap = df.collect().map(_.size)
    assert(!valueMap.exists(_ != 3))
  }

  it should "have same column type as Expected" in {
    val valueMap = df.schema
    for (typeName <- valueMap) {
      typeName.name match {
        case "blocknumber" => assert(typeName.dataType.equals(DecimalType(38, 0)))
        case "hash" => assert(typeName.dataType.equals(StringType))
        case "transactions" => assert(typeName.dataType.equals(ArrayType(TransactionUTD, true)))
      }
    }
  }

  it should "give transaction dataframe" in {
    readConf = ReadConf(Some(5), None, "Select * FROM transaction where blocknumber > 2245600 and blocknumber < 2245610 or hash='0x8cbb2b443a66fcbfb43e30247dea5de521293d9c64f00631f804e3f4ea79bdca'")(ethPartitioner)
    val option = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234")
    val df = spark.read.format("org.apache.spark.sql.eth").options(option).load()
    df.show(50)
    assertResult(30)(df.count())
    assertResult(17)(df.columns.size)
  }

}