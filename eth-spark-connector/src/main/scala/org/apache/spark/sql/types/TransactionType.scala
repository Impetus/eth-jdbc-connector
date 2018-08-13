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
package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.util.{ ArrayData, GenericArrayData }
import org.apache.spark.unsafe.types.UTF8String
import org.web3j.protocol.core.methods.response._

@SQLUserDefinedType(udt = classOf[TransactionUTD])
class TransactionType(@transient val transaction: Transaction) extends Serializable {

  override def hashCode(): Int = transaction.hashCode()

  override def equals(other: Any): Boolean = other match {
    case that: TransactionType => this.transaction == that.transaction
    case _ => false
  }

  val blockHash = transaction.getBlockHash
  val blockNumber = transaction.getBlockNumber.toString
  val creates = transaction.getCreates
  val from = transaction.getFrom
  val gas = transaction.getGas.toString
  val gasPrice = transaction.getGasPrice.toString
  val hash = transaction.getHash
  val input = transaction.getInput
  val nonce = transaction.getNonce.toString
  val publicKey = transaction.getPublicKey
  val r = transaction.getR
  val raw = transaction.getRaw
  val s = transaction.getS
  val to = transaction.getTo
  val transactionIndex = transaction.getTransactionIndex.toString
  val v = String.valueOf(transaction.getV)
  val value = transaction.getValue.toString

  override def toString = s"($blockHash, $blockNumber, $creates, $from, $gas, $gasPrice, $hash, $input, $nonce, $publicKey, $r, $raw, $s, $to, $transactionIndex, $v, $value)"
}

private[sql] class TransactionUTD extends UserDefinedType[TransactionType] {

  override def sqlType: DataType = ArrayType(StringType, false)

  override def serialize(p: TransactionType): GenericArrayData = {
    val output = new Array[Any](17)
    val txnInfo = p.transaction
    output(0) = UTF8String.fromString(txnInfo.getHash)
    output(1) = UTF8String.fromString(txnInfo.getNonceRaw)
    output(2) = UTF8String.fromString(txnInfo.getBlockHash)
    output(3) = UTF8String.fromString(txnInfo.getBlockNumberRaw)
    output(4) = UTF8String.fromString(txnInfo.getTransactionIndexRaw)
    output(5) = UTF8String.fromString(txnInfo.getFrom)
    output(6) = UTF8String.fromString(txnInfo.getTo)
    output(7) = UTF8String.fromString(txnInfo.getValueRaw)
    output(8) = UTF8String.fromString(txnInfo.getGasPriceRaw)
    output(9) = UTF8String.fromString(txnInfo.getGasRaw)
    output(10) = UTF8String.fromString(txnInfo.getInput)
    output(11) = UTF8String.fromString(txnInfo.getCreates)
    output(12) = UTF8String.fromString(txnInfo.getPublicKey)
    output(13) = UTF8String.fromString(txnInfo.getRaw)
    output(14) = UTF8String.fromString(txnInfo.getR)
    output(15) = UTF8String.fromString(txnInfo.getS)
    output(16) = UTF8String.fromString(txnInfo.getV.toString)
    new GenericArrayData(output)
  }

  override def deserialize(datum: Any): TransactionType = {
    datum match {
      case values: ArrayData =>
        val trn = new Transaction(
          if (values.getUTF8String(0) != null) values.getUTF8String(0).toString else null,
          if (values.getUTF8String(1) != null) values.getUTF8String(1).toString else "0x0",
          if (values.getUTF8String(2) != null) values.getUTF8String(2).toString else null,
          if (values.getUTF8String(3) != null) values.getUTF8String(3).toString else "0x0",
          if (values.getUTF8String(4) != null) values.getUTF8String(4).toString else "0x0",
          if (values.getUTF8String(5) != null) values.getUTF8String(5).toString else null,
          if (values.getUTF8String(6) != null) values.getUTF8String(6).toString else null,
          if (values.getUTF8String(7) != null) values.getUTF8String(7).toString else "0x0",
          if (values.getUTF8String(8) != null) values.getUTF8String(8).toString else "0x0",
          if (values.getUTF8String(9) != null) values.getUTF8String(9).toString else "0x0",
          if (values.getUTF8String(10) != null) values.getUTF8String(10).toString else null,
          if (values.getUTF8String(11) != null) values.getUTF8String(11).toString else null,
          if (values.getUTF8String(12) != null) values.getUTF8String(12).toString else null,
          if (values.getUTF8String(13) != null) values.getUTF8String(13).toString else null,
          if (values.getUTF8String(14) != null) values.getUTF8String(14).toString else null,
          if (values.getUTF8String(15) != null) values.getUTF8String(15).toString else null,
          if (values.getUTF8String(15) != null) values.getUTF8String(16).toString.toInt else 0)
        new TransactionType(trn)
    }
  }

  override def userClass: Class[TransactionType] = classOf[TransactionType]

  private[spark] override def asNullable: TransactionUTD = this
}

case object TransactionUTD extends TransactionUTD