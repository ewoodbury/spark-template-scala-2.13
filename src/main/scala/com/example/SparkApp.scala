package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

case class Person(name: String, age: Int)

object SparkApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Scala 2.13 Spark Template")
      .master("local[*]") // Remove this line when running on cluster
      .getOrCreate()

    try {
      // Example: Create a simple DataFrame
      val data = Seq(
        ("Alice", 25),
        ("Bob", 30),
        ("Charlie", 35)
      )
      
      // Define schema explicitly for better performance and type safety
      val schema = StructType(Seq(
        StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false)
      ))
      
      // Convert to Rows and create DataFrame
      val rows = data.map { case (name, age) => Row(name, age) }
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      
      println("Sample DataFrame:")
      df.show()
      
      // Example: Simple transformation
      val adults = df.filter(df("age") >= 18)
      println("Adults:")
      adults.show()
      
      // Example: Using implicits for easier DataFrame operations
      import spark.implicits._
      
      // Alternative approach using case classes (Scala 2.13 style)
      val peopleData = Seq(Person("Alice", 25), Person("Bob", 30), Person("Charlie", 35))
      val peopleDs = peopleData.toDS()
      
      println("Dataset with case class:")
      peopleDs.show()
      
      // Typed operations
      val adultNames = peopleDs.filter(_.age >= 18).map(_.name)
      println("Adult names:")
      adultNames.show()
      
    } finally {
      spark.stop()
    }
  }
}
