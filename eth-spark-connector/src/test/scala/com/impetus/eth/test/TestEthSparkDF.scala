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

import com.impetus.blkch.spark.connector.rdd.{ BlkchnRDD, ReadConf }
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner
import com.impetus.test.catagory.IntegrationTest
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.sql.types.{ ArrayType, DecimalType, StringType, TransactionUTD }
import org.scalatest.{ BeforeAndAfter, FlatSpec }

@IntegrationTest
class TestEthSparkDF extends FlatSpec with BeforeAndAfter with SharedSparkSession {

  //var spark: SparkSession = null
  val ethPartitioner: BlkchnPartitioner = DefaultEthPartitioner
  var readConf: ReadConf = null
  var df: DataFrame = null

  before {
    //spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
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
    for (typeName <- valueMap) {
      typeName.name match {
        case "blocknumber" => assert(typeName.dataType.equals(DecimalType(38, 0)))
        case "hash" => assert(typeName.dataType.equals(StringType))
        case "transactions" => assert(typeName.dataType.equals(ArrayType(TransactionUTD, true)))
      }
    }
    //spark.stop()
  }

}
