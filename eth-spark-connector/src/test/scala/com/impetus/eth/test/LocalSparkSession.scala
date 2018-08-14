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

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.Suite

trait LocalSparkSession extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

  @transient var spark: SparkSession = _

  override def beforeAll() {
    super.beforeAll()
  }

  override def afterEach() {
    resetSparkContext()
    super.afterEach()
  }

  def resetSparkContext(): Unit = {
    LocalSparkSession.stop(spark)
    spark = null
  }
}

object LocalSparkSession {
  def stop(spark: SparkSession) {
    if (spark != null) {
      spark.stop()
    }
  }

  def withSpark[T](spark: SparkSession)(f: SparkSession => T): T = {
    try {
      f(spark)
    } finally {
      stop(spark)
    }
  }
}
