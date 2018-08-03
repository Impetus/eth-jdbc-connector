package com.impetus.eth.spark.connector.rdd.partitioner

import com.impetus.blkch.spark.connector.BlkchnConnector
import com.impetus.blkch.spark.connector.rdd.ReadConf
import com.impetus.blkch.sql.query.RangeNode
import scala.collection.mutable.ArrayBuffer
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartition
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.blkch.util.{Range => BlkchRange}
import java.math.BigInteger
import scala.collection.JavaConversions._

class DefaultEthPartitioner extends BlkchnPartitioner {

  override def getPartitions(connector: BlkchnConnector, readConf: ReadConf): Array[BlkchnPartition] = {
    val rangesNodes = connector.withStatementDo{
      stat =>
        val rangesNode =stat.getProbableRange(readConf.query)
        rangesNode
    }

    def getRowCountInRange (range: (Long,Long)) = (range._2 - range._1) + 1

    val rangesTuple = rangesNodes.getRangeList.getRanges.toList.map{
      x =>
        val min:Long =x.getMin.asInstanceOf[BigInteger].longValue()
        val max:Long = x.getMax.asInstanceOf[BigInteger].longValue()
        println(min, max)
        (min, max)
    }

    val rangeSumList = rangesTuple.map{ x => getRowCountInRange(x._1,x._2)}

    val rangesTotalSum = rangeSumList.sum

    def getRanges(rows:Long,split: Long) = {
      var ranges:List[Long] = rangeSumList
      var currRows = rangesTuple
      for(i <- 0l until split) yield {
        var currRowsSum = 0l
        var currRowsList = new scala.collection.mutable.ListBuffer[(Long, Long)]
        while (currRowsSum < rows && !currRows.isEmpty) {
          if (ranges.head <= rows - currRowsSum) {
            currRowsSum += ranges.head
            ranges = ranges.tail
            currRowsList = currRowsList :+ (currRows.head._1, currRows.head._2)
            currRows = currRows.tail
          } else if (ranges.head > rows - currRowsSum) {
            currRowsSum += rows
            ranges = (ranges.head - rows) :: ranges.tail
            currRowsList = currRowsList :+ (currRows.head._1, currRows.head._1 + rows - 1)
            currRows = (currRows.head._1 + rows, currRows.head._2) :: currRows.tail
          }
        }
        getRangeNodeFromRanges(currRowsList.toList)
      }
    }

    def getRangeNodeFromRanges(rngLst: List[(Long,Long)]) ={
      /*Passing table name null and call setTableName function from physical plan paginate method*/
      val rangeNode = new RangeNode[BigInteger]("","blocknumber")
      for((x,y) <- rngLst){
        rangeNode.getRangeList.addRange(new BlkchRange[BigInteger](new BigInteger(x.toString), new BigInteger(y.toString)))
      }
      rangeNode
    }

    var buffer = ArrayBuffer[BlkchnPartition]()
    val start = new BigInteger("1")
    readConf.splitCount match {
      case Some(split) =>
        require(split > 0, s"Split should be positive : $split")
        val partitionRowCount = rangesTotalSum / split
        val rangeNodes = getRanges(partitionRowCount, split)
        for((rangenode,i) <- rangeNodes.zipWithIndex){
          buffer = buffer :+ new BlkchnPartition(i, rangenode, readConf)
        }

      case None =>
        readConf.fetchSizeInRows match {
          case Some(rowSize) =>
            require(rowSize > 0, s"Row Size should be positive : $rowSize")
            val split = if((rangesTotalSum / rowSize) * rowSize < rangesTotalSum) (rangesTotalSum / rowSize) + 1 else (rangesTotalSum / rowSize)
            val rangeNodes = getRanges(rowSize, split)
            for((rangenode,i) <- rangeNodes.zipWithIndex){
              rangenode.getRangeList.getRanges.toList.map {
                x =>
                  val min: Long = x.getMin.longValue()
                  val max: Long = x.getMax.longValue()
                  println(i, min, max)
              }
              buffer = buffer :+ new BlkchnPartition(i, rangenode, readConf)
            }

          case None =>
            /*Passing table name null and call setTableName function from physical plan paginate method*/
            val rangeNode = new RangeNode[BigInteger]("","blocknumber")
            rangeNode.getRangeList.addRange(new BlkchRange[BigInteger](start, new BigInteger(String.valueOf(rangesTotalSum))))
            buffer = buffer :+ new BlkchnPartition(0, rangeNode, readConf)
        }
    }
    buffer.toArray
  }

  override def toString: String = this.getClass.getCanonicalName
}

case object DefaultEthPartitioner extends DefaultEthPartitioner
