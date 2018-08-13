package com.impetus.eth.test

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

trait ShearedSparkSession  extends BeforeAndAfterAll { self: Suite =>

  @transient private var _spark: SparkSession = _

  def spark: SparkSession = _spark

  //var conf = new SparkConf(false)

  override def beforeAll() {
    _spark = SparkSession.builder().master("local").appName("Test").getOrCreate()
    super.beforeAll()
  }

  override def afterAll() {
    LocalSparkSession.stop(_spark)
    _spark = null
    super.afterAll()
  }
}
