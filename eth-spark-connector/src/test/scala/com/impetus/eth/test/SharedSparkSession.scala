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
import org.scalatest.{ BeforeAndAfterAll, Suite }

trait SharedSparkSession extends BeforeAndAfterAll { self: Suite =>

  @transient private var _spark: SparkSession = _

  def spark: SparkSession = _spark

  override def beforeAll() {
    _spark = SparkSession.builder().master("local").config("spark.sql.warehouse.dir", "file:///tmp").appName("Test").getOrCreate()
    super.beforeAll()
  }

  override def afterAll() {
    LocalSparkSession.stop(_spark)
    _spark = null
    super.afterAll()
  }
}
