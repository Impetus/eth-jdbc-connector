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
import org.apache.spark.sql.{ Row }
import org.apache.spark.sql.eth.EthSpark
import org.scalatest.{ BeforeAndAfter, FlatSpec }

@IntegrationTest
class TestEthSpark extends FlatSpec with BeforeAndAfter with SharedSparkSession {

  //var spark: SparkSession = null
  val ethPartitioner: BlkchnPartitioner = DefaultEthPartitioner
  var readConf: ReadConf = null
  var rdd: BlkchnRDD[Row] = null

  before {
    //spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    readConf = ReadConf(Some(3), None, "Select * FROM block where blocknumber > 1 and blocknumber < 30")(ethPartitioner)
    rdd = EthSpark.load[Row](spark.sparkContext, readConf, Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    rdd.cache()
  }

  "Eth Spark" should "have Read Conf" in {
    assert(readConf != null)
  }

  it should "have rdd with same partition as specified" in {
    assert(rdd.getNumPartitions == 3)
  }

  it should "have not empty rdd" in {
    assert(rdd.collect().isEmpty != true)
  }

  it should "be able to create data frame" in {
    val option = readConf.asOptions() ++ Map("url" -> "jdbc:blkchn:ethereum://172.25.41.52:8545")
    val df = spark.read.format("org.apache.spark.sql.eth").options(option).load()
    assert(df.select(df.col("blocknumber")).collect().length > 0)
  }

  "Read Conf asOption" should "return Default Partition Name" in {
    val option = readConf.asOptions()
    assert(option.getOrElse("spark.blkchn.partitioner", "").toString.equals(ethPartitioner.getClass.getCanonicalName.replaceAll("\\$", "")))
  }
}
