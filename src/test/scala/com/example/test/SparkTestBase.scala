package com.example.test

import org.apache.spark.sql.Dataset
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import sparkutils.TestSparkPlatform

trait SparkTestBase extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Set local mode for testing
    val _ = System.setProperty("SPARK_MODE", "local")
    val _ = System.setProperty("sbt.testing", "true")
  }
  
  override def beforeEach(): Unit = {
    super.beforeEach()
    // Clear local tables for this test context
    TestSparkPlatform.clearLocalTables()
  }
  
  override def afterEach(): Unit = {
    // Clean up after each test to prevent interference
    TestSparkPlatform.clearLocalTables()
    super.afterEach()
  }
  
  override def afterAll(): Unit = {
    // Clean up the entire test context but don't stop the shared session
    TestSparkPlatform.clearLocalTables()
    super.afterAll()
  }
  
  // Use our thread-safe test platform
  lazy val spark = TestSparkPlatform.spark
  
  // Helper method to create test datasets
  def createTestDataset[T](data: Seq[T])(implicit encoder: org.apache.spark.sql.Encoder[T]): Dataset[T] = {
    import spark.implicits._
    data.toDS()
  }
}
