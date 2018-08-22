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

import java.math.BigInteger

import com.impetus.blkch.spark.connector.rdd.ReadConf
import com.impetus.blkch.spark.connector.BlkchnConnector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.eth.EthConnectorConf
import org.scalatest.{ BeforeAndAfter, FlatSpec }
import com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner
import com.impetus.test.catagory.{ UnitTest }

@UnitTest
class TestDefaultEthPartitioner extends FlatSpec with BeforeAndAfter with SharedSparkSession {

  var ethConnectorConf: EthConnectorConf = null
  var blkchnConnector: BlkchnConnector = null
  var defaultPartition: DefaultEthPartitioner = null

  before {
    Class.forName("com.impetus.eth.jdbc.EthDriver")
    ethConnectorConf = EthConnectorConf(spark.sparkContext.getConf, Map("url" -> "jdbc:blkchn:ethereum://ropsten.infura.io/1234"))
    blkchnConnector = new BlkchnConnector(ethConnectorConf)
    defaultPartition = DefaultEthPartitioner
  }

  "Default Eth Partition" should "give same number of partition as specified" in {
    val readConf = ReadConf(Some(3), None, "Select * FROM block")(defaultPartition)
    val bufferOfRange = defaultPartition.getPartitions(blkchnConnector, readConf)
    assert(bufferOfRange.size == 3)
  }

  it should "give same number of line in partition if specified fetchSizeInRows" in {
    val readConf = ReadConf(None, Some(300000), "Select * FROM block")(defaultPartition)
    val bufferOfRange = defaultPartition.getPartitions(blkchnConnector, readConf)
    val max = bufferOfRange(0).range.getRangeList.getRanges.get(0).getMax.asInstanceOf[BigInteger]
    val min = bufferOfRange(0).range.getRangeList.getRanges.get(0).getMin.asInstanceOf[BigInteger]
    assert(max.subtract(min).intValue() + 1 == 300000)
  }

  it should "give range size" in {
    val readConf = ReadConf(Some(5), None, "Select * FROM transaction where blocknumber > 12345 and blocknumber <12390")(defaultPartition)
    val bufferOfRange = defaultPartition.getPartitions(blkchnConnector, readConf)
    assertResult(5)(bufferOfRange.size)
  }

  it should "give partition value" in {
    val readConf = ReadConf(Some(5), None, "Select * FROM transaction where blocknumber > 12345 and blocknumber <12390")(defaultPartition)
    val bufferOfRange = defaultPartition.getPartitions(blkchnConnector, readConf)
    assertResult("12346")(bufferOfRange(0).range.getRangeList.getRanges.get(0).getMin.toString())
  }

}
