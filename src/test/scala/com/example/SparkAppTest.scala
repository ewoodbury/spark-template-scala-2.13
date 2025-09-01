package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll

class SparkAppTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  
  var spark: SparkSession = _
  
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
      .getOrCreate()
  }
  
  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
  
  "SparkApp" should "create a DataFrame with correct schema" in {
    val data = Seq(("Alice", 25), ("Bob", 30))
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false)
    ))
    
    val rows = data.map { case (name, age) => org.apache.spark.sql.Row(name, age) }
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    
    df.count() should be(2)
    df.schema should be(schema)
  }
  
  it should "filter adults correctly" in {
    val sparkSession = spark
    import sparkSession.implicits._
    
    val data = Seq(Person("Alice", 25), Person("Bob", 17), Person("Charlie", 30))
    val df = data.toDF()
    
    val adults = df.filter($"age" >= 18)
    adults.count() should be(2)
    
    val adultNames = adults.select("name").as[String].collect()
    adultNames should contain allElementsOf Seq("Alice", "Charlie")
  }
}
