package org.apache.spark.sql.eth

import com.impetus.blkch.spark.connector.BlkchnConnector
import com.impetus.blkch.spark.connector.rdd.partitioner.BlkchnPartitioner
import com.impetus.blkch.spark.connector.rdd.{BlkchnRDD, EthRDD, ReadConf}
import com.impetus.eth.spark.connector.rdd.partitioner.DefaultEthPartitioner
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.ClassTag

case class EthSpark(sparkSession: SparkSession, connector: BlkchnConnector, readConf: ReadConf) {

  private def rdd[D: ClassTag]: BlkchnRDD[D] = {
    new EthRDD[D](sparkSession.sparkContext, sparkSession.sparkContext.broadcast(connector), readConf)
  }

  def toRDD[D: ClassTag]: BlkchnRDD[D] = rdd[D]

  def toDF(): DataFrame = {
    sparkSession.read.format(EthFormat)
      .options(readConf.asOptions()).load()
  }
}

object EthSpark {

  def builder(): Builder = new Builder

  def load[D: ClassTag](sc: SparkContext): BlkchnRDD[D] = load(sc, ReadConf(sc.conf))

  def load[D: ClassTag](sc: SparkContext, readConf: ReadConf): BlkchnRDD[D] = load(sc, readConf, Map())

  def load[D: ClassTag](sc: SparkContext, readConf: ReadConf, options: Map[String, String]): BlkchnRDD[D] = {
    builder().sc(sc).readConf(readConf).options(options).build().toRDD
  }

  def insertTransaction(readConf: ReadConf, options: Map[String, String]) ={
    val ethCon = new BlkchnConnector(EthConnectorConf(new SparkConf(),options ++ readConf.asOptions()))
    val transactionStatus = ethCon.withStatementDo(stat => stat.execute(readConf.query))
    transactionStatus
  }

  def load(spark: SparkSession): DataFrame = builder().sparkSession(spark).build().toDF()

  class Builder {

    private var sparkSession: Option[SparkSession] = None
    private var connector: Option[BlkchnConnector] = None
    private var readConfig: Option[ReadConf] = None
    private var options: Map[String, String] = Map()

    def sparkSession(sparkSession: SparkSession): Builder = {
      this.sparkSession = Some(sparkSession)
      this
    }

    def sc(sc: SparkContext): Builder = {
      this.sparkSession = Some(SparkSession.builder().config(sc.getConf).getOrCreate())
      this
    }

    def connector(connector: BlkchnConnector): Builder = {
      this.connector = Some(connector)
      this
    }

    def readConf(readConf: ReadConf): Builder = {
      this.readConfig = Some(readConf)
      this
    }

    def option(key: String, value: String): Builder = {
      this.options = this.options + (key -> value)
      this
    }

    def options(options: Map[String, String]): Builder = {
      this.options = options
      this
    }

    def build(): EthSpark = {
      require(sparkSession.isDefined, "The SparkSession must be set, either explicitly or via the SparkContext")
      val session = sparkSession.get
      val readConf = readConfig match {
        case Some(config) => config
        case None => ReadConf(session.sparkContext.conf, options)
      }
      val conn = connector match {
        case Some(connect) => connect
        case None => new BlkchnConnector(EthConnectorConf(session.sparkContext.conf, options))
      }
      new EthSpark(session, conn, readConf)
    }
  }

  object implicits extends Serializable {
    implicit def getEthPartitioner: BlkchnPartitioner = DefaultEthPartitioner
  }
}
