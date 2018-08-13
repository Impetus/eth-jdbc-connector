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
