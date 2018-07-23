package org.apache.spark.sql.eth

import java.sql.DriverManager
import java.util.Properties
import com.impetus.blkch.jdbc.BlkchnConnection
import com.impetus.blkch.spark.connector.BlkchnConnectorConf
import com.impetus.blkch.spark.connector.util.ConfigParam
import com.impetus.eth.jdbc.DriverConstants
import org.apache.spark.SparkConf

class EthConnectorConf (conf: SparkConf, options: Map[String, String]) extends BlkchnConnectorConf(conf)  {

  val connectionURL = conf.get(EthConnectorConf.connectionURL.name,
    options.getOrElse(EthConnectorConf.connectionURL.name,EthConnectorConf.connectionURL.default))

  val keystorePath = conf.get(EthConnectorConf.keystorePath.name,
    options.getOrElse(EthConnectorConf.keystorePath.name,EthConnectorConf.keystorePath.default))

  val keystorePassword = conf.get(EthConnectorConf.keystorePassword.name,
    options.getOrElse(EthConnectorConf.keystorePassword.name,EthConnectorConf.keystorePassword.default))


  Class.forName("com.impetus.eth.jdbc.EthDriver")

  override def getConnection(): BlkchnConnection = {
    val prop = new Properties
    prop.put(DriverConstants.KEYSTORE_PATH, keystorePath)
    prop.put(DriverConstants.KEYSTORE_PASSWORD, keystorePassword)

    val jdbcUrl = connectionURL
    DriverManager.getConnection(jdbcUrl, prop).asInstanceOf[BlkchnConnection]
  }

  override def toString: String = {
    val sb = new StringBuilder("[")
    sb.append("[" + EthConnectorConf.connectionURL.name + ":" + connectionURL + "]")
    sb.append("[" + EthConnectorConf.keystorePath.name + ":" + keystorePath + "]")
    sb.append("[" + EthConnectorConf.keystorePassword.name + ":" + keystorePassword + "]")
    sb.append("]")
    sb.toString()
  }

}

object EthConnectorConf{

  val connectionURL = ConfigParam[String]("url","jdbc:blkchn:ethereum://127.0.0.1:8545",
    "Etheruem jdbc connection string default point to localhost")

  val keystorePath = ConfigParam[String](DriverConstants.KEYSTORE_PATH,"","KEYSTORE_PATH")

  val keystorePassword = ConfigParam[String](DriverConstants.KEYSTORE_PASSWORD,"","KEYSTORE_PASSWORD")

  def apply(conf: SparkConf): EthConnectorConf = new EthConnectorConf(conf, Map())

  def apply(conf: SparkConf, options: Map[String, String]): EthConnectorConf = new EthConnectorConf(conf, options)
}
