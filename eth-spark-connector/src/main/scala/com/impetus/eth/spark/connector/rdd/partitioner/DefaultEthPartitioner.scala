package com.impetus.eth.spark.connector.rdd.partitioner

import com.impetus.blkch.spark.connector.BlkchnConnector
import com.impetus.blkch.spark.connector.rdd.ReadConf
import com.impetus.blkch.sql.query.RangeNode
import scala.collection.mutable.ArrayBuffer
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartition
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.blkch.util.{Range => BlkchRange}
import java.math.BigInteger

class DefaultEthPartitioner extends BlkchnPartitioner {

  override def getPartitions(connector: BlkchnConnector, readConf: ReadConf): Array[BlkchnPartition] = {
    val rowCount = 1000/*connector.withStatementDo {
      stat =>
        val rs = stat.executeQuery("SELECT count(block_no) AS cnt FROM block WHERE block_no >= 1")
        rs.next() match {
          case true => rs.getLong("cnt")
          case false => 0l
        }
    }*/
    var buffer = ArrayBuffer[BlkchnPartition]()
    var start = new BigInteger("1")
    readConf.splitCount match {
      case Some(split) => val partitionRowCount = rowCount / split
        for(i <- 0 until split) {
          val rangeNode = new RangeNode[BigInteger]("block", "blocknumber")
          rangeNode.getRangeList.addRange(new BlkchRange[BigInteger](start, start.add(new BigInteger(String.valueOf(partitionRowCount)))))
          buffer = buffer :+ new BlkchnPartition(i, rangeNode, readConf)
          start = start.add(new BigInteger(String.valueOf(partitionRowCount + 1)));
        }

      case None =>
        readConf.fetchSizeInRows match {
          case Some(rowSize) => val split = (rowCount / rowSize).toInt
            for(i <- 0 until split) {
              val rangeNode = new RangeNode[BigInteger]("block", "blocknumber")
              rangeNode.getRangeList.addRange(new BlkchRange[BigInteger](start, start.add(new BigInteger(String.valueOf(rowSize)))))
              buffer = buffer :+ new BlkchnPartition(i.toInt, rangeNode, readConf)
              start = start.add(new BigInteger(String.valueOf(rowSize + 1)));
            }
          case None => val rangeNode = new RangeNode[BigInteger]("block", "blocknumber")
            rangeNode.getRangeList.addRange(new BlkchRange[BigInteger](start, new BigInteger(String.valueOf(rowCount))))
            buffer = buffer :+ new BlkchnPartition(0, rangeNode, readConf)
        }

    }
    buffer.toArray
  }
}

case object DefaultEthPartitioner extends DefaultEthPartitioner