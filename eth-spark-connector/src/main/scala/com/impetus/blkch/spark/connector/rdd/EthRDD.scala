package com.impetus.blkch.spark.connector.rdd


import java.sql.{ResultSetMetaData}
import com.impetus.blkch.spark.connector.{BlkchnConnector}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext}
import org.web3j.protocol.core.methods.response.EthBlock.TransactionResult
import org.web3j.protocol.core.methods.response.Transaction
import scala.collection.JavaConverters._

import scala.reflect.ClassTag

class EthRDD[R: ClassTag](@transient sc: SparkContext,
                          private[impetus] override val connector: Broadcast[BlkchnConnector],
                          private[impetus] override val readConf: ReadConf) extends BlkchnRDD[R](sc, connector, readConf){

  override def handleExtraType(index: Int, metadata: ResultSetMetaData, data: java.lang.Object) =
    if(data.isInstanceOf[java.util.ArrayList[_]]  && metadata.getColumnName(index).equalsIgnoreCase("transactions")){
      StructField(metadata.getColumnLabel(index), ArrayType(TransactionUTD, true), true)
    }else if(data.isInstanceOf[java.util.ArrayList[_]]){
      StructField(metadata.getColumnLabel(index), ArrayType(StringType, true), true)
    }else {
      StructField(metadata.getColumnLabel(index), StringType, true)
    }


  override def handleExtraData(index: Int, metadata: ResultSetMetaData, data: java.lang.Object): Any = if(data.isInstanceOf[java.util.ArrayList[_]]  && metadata.getColumnName(index).equalsIgnoreCase("transactions")){
    val transactionList = data.asInstanceOf[java.util.ArrayList[TransactionResult[Transaction]]].asScala.map(x => new TransactionType(x.get()))
    transactionList.asInstanceOf[Any]
  }else if(data.isInstanceOf[java.util.ArrayList[_]]){
    val strList = data.asInstanceOf[java.util.ArrayList[String]].asScala
    strList.asInstanceOf[Any]
  }else {
    data.asInstanceOf[Any]
  }

}
