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
package com.impetus.eth.spark.sql.JdbcDielect

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import com.impetus.blkch.BlkchnException
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.AnyDataType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.ObjectType
import org.apache.spark.sql.types.TransactionType
import org.apache.spark.sql.types.MetadataBuilder
import java.sql.SQLType
import java.sql.Types
import org.apache.spark.sql.types.FloatType
import org.apache.spark.sql.types.ObjectType
import org.apache.spark.sql.types.ObjectType


object EthereumDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:blkchn:ethereum")

  override def quoteIdentifier(colName: String): String = {
    s"$colName"
  }

  override def getCatalystType(
    sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.BIGINT)
      Some(DecimalType(38, 0))
    else if (sqlType == Types.VARCHAR)
      Some(StringType)
    else if (sqlType == Types.INTEGER)
      Some(IntegerType)
    else if (sqlType == Types.FLOAT)
      Some(FloatType)
    else if (sqlType == Types.DOUBLE)
      Some(DoubleType)
    else if (sqlType == Types.JAVA_OBJECT)
      Some(ArrayType(StringType,true))
    else
      Some(StringType)
  }

}