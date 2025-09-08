package com.example.test

import org.apache.spark.sql.Dataset
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.example.sparkutils.SparkPlatform

trait SparkTestBase extends AnyFlatSpec with Matchers with DatasetSuiteBase with BeforeAndAfterAll with BeforeAndAfterEach {
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Set local mode for testing
    val _ = System.setProperty("SPARK_MODE", "local")
  }
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    SparkPlatform.clearLocalTables()
  }
  
  override def afterAll(): Unit = {
    SparkPlatform.stop()
    super.afterAll()
  }
  
  // Helper method to create test datasets
  def createTestDataset[T](data: Seq[T])(implicit encoder: org.apache.spark.sql.Encoder[T]): Dataset[T] = {
    import spark.implicits._
    data.toDS()
  }
}
