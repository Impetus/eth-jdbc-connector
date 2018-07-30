package com.impetus.eth.test

import com.impetus.blkch.spark.connector.BlkchnConnector
import com.impetus.blkch.spark.connector.rdd.ReadConf
import com.impetus.blkch.sql.query.RangeNode
import scala.collection.mutable.ArrayBuffer
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartition
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.blkch.util.{Range => BlkchRange}
import java.math.BigInteger

class TestEthPartitioner extends BlkchnPartitioner {

  override def getPartitions(connector: BlkchnConnector, readConf: ReadConf): Array[BlkchnPartition] = {
    val rowCount = 50
    def getPatitionRange(partitionSize: Long, split: Long, start: Long = 0) = {
      var preY = start
      for(i <- 0l until split) yield{
        val x = preY + 1
        preY = partitionSize * (i + 1)
        val y = if(preY > rowCount) rowCount else if(i == split && preY < rowCount) rowCount else preY
        (new BigInteger(String.valueOf(x)),new BigInteger(String.valueOf(y)))
      }
    }
    var buffer = ArrayBuffer[BlkchnPartition]()
    val start = new BigInteger("1")
    readConf.splitCount match {
      case Some(split) =>
        val partitionRowCount = rowCount / split
        val partitionsRange = getPatitionRange(partitionRowCount,split)
        for(((startRange, endRange),i) <- partitionsRange.zipWithIndex){
          /*Passing table name null and call setTableName function in physical plan paginate method*/
          val rangeNode = new RangeNode[BigInteger]("","blocknumber")
          rangeNode.getRangeList.addRange(new BlkchRange[BigInteger](startRange, endRange))
          buffer = buffer :+ new BlkchnPartition(i, rangeNode, readConf)
        }

      case None =>
        readConf.fetchSizeInRows match {
          case Some(rowSize) =>
            val split = if((rowCount / rowSize) * rowSize < rowCount) (rowCount / rowSize) + 1 else (rowCount / rowSize)
            val partitionsRange = getPatitionRange(rowSize,split)
            for(((startRange, endRange),i) <- partitionsRange.zipWithIndex){
              /*Passing table name null and call setTableName function in physical plan paginate method*/
              val rangeNode = new RangeNode[BigInteger]("","blocknumber")
              rangeNode.getRangeList.addRange(new BlkchRange[BigInteger](startRange, endRange))
              buffer = buffer :+ new BlkchnPartition(i, rangeNode, readConf)
            }
          case None =>
            /*Passing table name null and call setTableName function in physical plan paginate method*/
            val rangeNode = new RangeNode[BigInteger]("","blocknumber")
            rangeNode.getRangeList.addRange(new BlkchRange[BigInteger](start, new BigInteger(String.valueOf(rowCount))))
            buffer = buffer :+ new BlkchnPartition(0, rangeNode, readConf)
        }

    }
    buffer.toArray
  }

  override def toString: String = this.getClass.getCanonicalName
}

case object TestEthPartitioner extends TestEthPartitioner
