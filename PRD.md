PRD to upgrade the Spark Template project to have a more meaningful example job, better spark utility functions, and an improved test setup

# Overview

## Utility Functions

- Add a `sparkutils` package with the following objects:
    - `SparkPlatform`: Initialize SparkSession with sensible defaults for production jobs
    - `FetchUtils`: `readTableAsDataFrame` and `readTableAsDataset`. `Dataset` version requires a input case class type and an implicit encoder.
    - `WriteUtils`: `writeDataFrameToTable` and `writeDatasetToTable`. `Dataset` version requires a input case class type and an implicit encoder.
- Support for either `production` mode or `local` mode.
    - When in local mode, the common functions write to local Spark session memory with a basic mutable map of (tableName -> DataFrame/Dataset). `read` methods simply read from this map based on the table name
    - When in production mode, the common functions read and write to Iceberg tables.

## Example Job
- Update the example job to do a simple ETL pipeline:
    - Read two different fact datasets from two source tables (we will run local mode using in-memory dfs for tests). Each source table should have between 5-10 columns.
    - Also read one lookup table (dimension table), such as `lookup_user` to join to the fact datasets.
    - Perform at least 4 simple transformations (filter based on partition column and other cols, apply some window function to get latest record per some id, rename some columns, join the two datasets, do some final dataset cleanup to prepare to write).
    - Write to a target table
- The job should used typed Datasets across the whole job. Each function should input a dataset of one type, and output a dataset of another type (or output the same type if there are no schema changes in this file).
- Job should take start and end dates as input args, as YYYYMMDD ints. It also takes the input and output table names as args.
    - Use `scopt` for CLI arg parsing
- The `main` file should only parse the CLI args, initialize the production Spark session, and then call the `run` function in the job file.
    - Any operations that are only possible on a production Spark cluster should be done here in this `main` function, since the main function is not run in the local mode unit/e2e tests.
- The job file should have a filename that matches the overall package name, e.g. `com.example.useractivity` has job file `UserActivity.scala`.
    - The job file should have only a single `run` method that calls the other transformation functions in order. This is the function that is called during the end-to-end pipeline test.

## Test Setup
- Within the `test` directory, override the `SparkPlatform`, `read`, and `write` functions to use local mode with the mutable map of data.
    - This will allow running the tests seamlessly, without needing to pollute the core code with if-statements to handle local mode. The methods should use the appropriate logic automatically.
    - Note that we will not mock any Spark operations. We will run the tests using a real SparkSession in local mode.
- For the test session, set a shared SparkSession that is used across all tests to speed up test runtime. Do not initialize a SparkSession in every file or in every test function.
- Add several unit test for each transformation function in the example job. These unit tests are where the business logic and edges cases will be tested extensively.
    - Unit tests must set up a basic input dataframe/dataset, call the transformation function, and then check that the output dataframe/dataset matches the expected output.
    - Build helper functions to shorten the code to set up input dataframes
    - Add the library `spark-testing-base` to help with dataframe comparisons and general testing.
- Add an end-to-end pipeline test that runs the example job in local mode, and compares the output dataset to the expected output dataset. Here is the sequence for e2e tests:
    - Set up the input tables with dataframe/dataset initialization code. Write input tables using the `write` utility function (which will write to the in-memory map in local mode).
    - Set up the job input args
    - Call the `run` function from the job. The run function will automatically write to the output table using the `write` utility function, which is set to write to the map since we're in local mode.
    - Set the expected output table as a dataframe/dataset
    - Read the output table using the `read` utility function (which reads from the map in local mode)
    - assertDataFrameEquals to compare the output table to the expected output table.
    - Note that these e2e tests should take around 2-5 rows of data, so the test code is concise and easy to read in a single file. An engineer should be able to read and understand the entire e2e test without even scrolling, let along jumping between multiple files.


# Implementation Plan

## 1. Core Infrastructure and Utilities

### SparkPlatform Object
Creates a standardized SparkSession with production-ready configurations.

```scala
package com.example.sparkutils

import org.apache.spark.sql.SparkSession
import scala.collection.mutable

object SparkPlatform {
  private var _spark: Option[SparkSession] = None
  private val _localTables: mutable.Map[String, org.apache.spark.sql.Dataset[_]] = mutable.Map.empty
  
  def isLocal: Boolean = sys.env.getOrElse("SPARK_MODE", "production") == "local"
  
  def spark: SparkSession = {
    _spark.getOrElse {
      val session = if (isLocal) {
        SparkSession.builder()
          .appName("Local Spark Template")
          .master("local[*]")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .getOrCreate()
      } else {
        SparkSession.builder()
          .appName("Production Spark Template")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
          .config("spark.sql.catalog.spark_catalog.type", "hive")
          .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
          .getOrCreate()
      }
      _spark = Some(session)
      session
    }
  }
  
  def getLocalTable(tableName: String): Option[org.apache.spark.sql.Dataset[_]] = {
    _localTables.get(tableName)
  }
  
  def setLocalTable(tableName: String, dataset: org.apache.spark.sql.Dataset[_]): Unit = {
    _localTables(tableName) = dataset
  }
  
  def clearLocalTables(): Unit = _localTables.clear()
  
  def stop(): Unit = {
    _spark.foreach(_.stop())
    _spark = None
    clearLocalTables()
  }
}
```

### FetchUtils Object
Provides unified read functionality for both local and production modes.

```scala
package com.example.sparkutils

import org.apache.spark.sql.{Dataset, DataFrame, Encoder}

object FetchUtils {
  import SparkPlatform.spark
  
  def readTableAsDataFrame(tableName: String): DataFrame = {
    if (SparkPlatform.isLocal) {
      SparkPlatform.getLocalTable(tableName)
        .getOrElse(throw new RuntimeException(s"Table $tableName not found in local storage"))
        .toDF()
    } else {
      spark.table(tableName)
    }
  }
  
  def readTableAsDataset[T](tableName: String)(implicit encoder: Encoder[T]): Dataset[T] = {
    if (SparkPlatform.isLocal) {
      SparkPlatform.getLocalTable(tableName)
        .getOrElse(throw new RuntimeException(s"Table $tableName not found in local storage"))
        .as[T]
    } else {
      spark.table(tableName).as[T]
    }
  }
}
```

### WriteUtils Object
Provides unified write functionality for both local and production modes.

```scala
package com.example.sparkutils

import org.apache.spark.sql.{Dataset, DataFrame, SaveMode}

object WriteUtils {
  def writeDataFrameToTable(df: DataFrame, tableName: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    if (SparkPlatform.isLocal) {
      SparkPlatform.setLocalTable(tableName, df)
    } else {
      df.writeTo(tableName).using("iceberg").createOrReplace()
    }
  }
  
  def writeDatasetToTable[T](ds: Dataset[T], tableName: String, mode: SaveMode = SaveMode.Overwrite): Unit = {
    if (SparkPlatform.isLocal) {
      SparkPlatform.setLocalTable(tableName, ds)
    } else {
      ds.writeTo(tableName).using("iceberg").createOrReplace()
    }
  }
}
```

## 2. Data Models and Schema Definitions

### Input Schema Definitions
```scala
package com.example.useractivity.model

import java.sql.Timestamp

// Source Table 1: User Events
case class UserEvent(
  event_id: String,
  user_id: String,
  event_type: String,
  event_timestamp: Timestamp,
  session_id: String,
  page_url: String,
  device_type: String,
  partition_date: Int // YYYYMMDD format
)

// Source Table 2: Purchase Transactions
case class PurchaseTransaction(
  transaction_id: String,
  user_id: String,
  product_id: String,
  purchase_amount: Double,
  purchase_timestamp: Timestamp,
  payment_method: String,
  currency: String,
  is_refunded: Boolean,
  partition_date: Int // YYYYMMDD format
)

// Lookup Table: User Profiles
case class UserProfile(
  user_id: String,
  username: String,
  email: String,
  registration_date: Timestamp,
  age_group: String,
  country: String,
  subscription_tier: String,
  is_active: Boolean
)
```

### Intermediate Schema Definitions
```scala
// After filtering and deduplication
case class FilteredUserEvent(
  event_id: String,
  user_id: String,
  event_type: String,
  event_timestamp: Timestamp,
  session_id: String,
  page_url: String,
  device_type: String,
  partition_date: Int,
  row_number: Long
)

case class FilteredPurchaseTransaction(
  transaction_id: String,
  user_id: String,
  product_id: String,
  purchase_amount: Double,
  purchase_timestamp: Timestamp,
  payment_method: String,
  currency: String,
  is_refunded: Boolean,
  partition_date: Int,
  row_number: Long
)

// After joining with user profiles
case class EnrichedUserEvent(
  event_id: String,
  user_id: String,
  username: String,
  event_type: String,
  event_timestamp: Timestamp,
  session_id: String,
  page_url: String,
  device_type: String,
  age_group: String,
  country: String,
  subscription_tier: String,
  partition_date: Int
)

case class EnrichedPurchaseTransaction(
  transaction_id: String,
  user_id: String,
  username: String,
  product_id: String,
  purchase_amount: Double,
  purchase_timestamp: Timestamp,
  payment_method: String,
  currency: String,
  is_refunded: Boolean,
  age_group: String,
  country: String,
  subscription_tier: String,
  partition_date: Int
)
```

### Final Output Schema
```scala
// Final aggregated user activity summary
case class UserActivitySummary(
  user_id: String,
  username: String,
  age_group: String,
  country: String,
  subscription_tier: String,
  total_events: Long,
  unique_sessions: Long,
  total_purchases: Long,
  total_purchase_amount: Double,
  avg_purchase_amount: Double,
  most_common_device: String,
  most_common_event_type: String,
  first_event_timestamp: Timestamp,
  last_event_timestamp: Timestamp,
  partition_date: Int
)
```

## 3. Transformation Functions

### Core ETL Pipeline Functions
```scala
package com.example.useractivity.transformations

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.example.useractivity.model._

object UserActivityTransformations {
  
  def filterAndDedupeUserEvents(
    userEvents: Dataset[UserEvent], 
    startDate: Int, 
    endDate: Int
  )(implicit spark: SparkSession): Dataset[FilteredUserEvent] = {
    import spark.implicits._
    
    val window = Window.partitionBy("user_id", "session_id", "event_type")
      .orderBy(desc("event_timestamp"))
    
    userEvents
      .filter(col("partition_date") >= startDate && col("partition_date") <= endDate)
      .filter(col("event_type").isNotNull && col("user_id").isNotNull)
      .withColumn("row_number", row_number().over(window))
      .as[FilteredUserEvent]
  }
  
  def filterAndDedupePurchases(
    purchases: Dataset[PurchaseTransaction], 
    startDate: Int, 
    endDate: Int
  )(implicit spark: SparkSession): Dataset[FilteredPurchaseTransaction] = {
    import spark.implicits._
    
    val window = Window.partitionBy("transaction_id")
      .orderBy(desc("purchase_timestamp"))
    
    purchases
      .filter(col("partition_date") >= startDate && col("partition_date") <= endDate)
      .filter(!col("is_refunded"))
      .filter(col("purchase_amount") > 0)
      .withColumn("row_number", row_number().over(window))
      .as[FilteredPurchaseTransaction]
  }
  
  def enrichWithUserProfiles[T](
    dataset: Dataset[T], 
    userProfiles: Dataset[UserProfile]
  )(implicit spark: SparkSession): Dataset[_] = {
    // This would be implemented specifically for each input type
    // Due to Scala's type system limitations, we'd have separate methods for each type
    ???
  }
  
  def enrichUserEventsWithProfiles(
    userEvents: Dataset[FilteredUserEvent], 
    userProfiles: Dataset[UserProfile]
  )(implicit spark: SparkSession): Dataset[EnrichedUserEvent] = {
    import spark.implicits._
    
    userEvents
      .filter(col("row_number") === 1)
      .drop("row_number")
      .join(userProfiles.filter(col("is_active")), Seq("user_id"), "left")
      .select(
        col("event_id"),
        col("user_id"),
        col("username"),
        col("event_type"),
        col("event_timestamp"),
        col("session_id"),
        col("page_url"),
        col("device_type"),
        col("age_group"),
        col("country"),
        col("subscription_tier"),
        col("partition_date")
      )
      .as[EnrichedUserEvent]
  }
  
  def enrichPurchasesWithProfiles(
    purchases: Dataset[FilteredPurchaseTransaction], 
    userProfiles: Dataset[UserProfile]
  )(implicit spark: SparkSession): Dataset[EnrichedPurchaseTransaction] = {
    import spark.implicits._
    
    purchases
      .filter(col("row_number") === 1)
      .drop("row_number")
      .join(userProfiles.filter(col("is_active")), Seq("user_id"), "left")
      .select(
        col("transaction_id"),
        col("user_id"),
        col("username"),
        col("product_id"),
        col("purchase_amount"),
        col("purchase_timestamp"),
        col("payment_method"),
        col("currency"),
        col("is_refunded"),
        col("age_group"),
        col("country"),
        col("subscription_tier"),
        col("partition_date")
      )
      .as[EnrichedPurchaseTransaction]
  }
  
  def aggregateUserActivity(
    enrichedEvents: Dataset[EnrichedUserEvent],
    enrichedPurchases: Dataset[EnrichedPurchaseTransaction]
  )(implicit spark: SparkSession): Dataset[UserActivitySummary] = {
    import spark.implicits._
    
    val eventAggs = enrichedEvents
      .groupBy("user_id", "username", "age_group", "country", "subscription_tier", "partition_date")
      .agg(
        count("*").as("total_events"),
        countDistinct("session_id").as("unique_sessions"),
        mode("device_type").as("most_common_device"),
        mode("event_type").as("most_common_event_type"),
        min("event_timestamp").as("first_event_timestamp"),
        max("event_timestamp").as("last_event_timestamp")
      )
    
    val purchaseAggs = enrichedPurchases
      .groupBy("user_id")
      .agg(
        count("*").as("total_purchases"),
        sum("purchase_amount").as("total_purchase_amount"),
        avg("purchase_amount").as("avg_purchase_amount")
      )
    
    eventAggs
      .join(purchaseAggs, Seq("user_id"), "left")
      .na.fill(0, Seq("total_purchases", "total_purchase_amount", "avg_purchase_amount"))
      .as[UserActivitySummary]
  }
}
```

## 4. Main Job Implementation

### Updated build.sbt Dependencies
```scala
libraryDependencies ++= Seq(
  // Existing dependencies...
  
  // Add for testing
  "com.holdenkarau" %% "spark-testing-base" % "3.5.1_1.4.7" % Test,
  
  // Add for Iceberg support (production)
  "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.13" % "1.4.2" % "provided"
)
```

### Main Application Entry Point
```scala
package com.example.useractivity

import com.example.sparkutils.SparkPlatform

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length != 5) {
      System.err.println("Usage: Main <startDate> <endDate> <userEventsTable> <purchasesTable> <outputTable>")
      System.exit(1)
    }
    
    val startDate = args(0).toInt
    val endDate = args(1).toInt
    val userEventsTable = args(2)
    val purchasesTable = args(3)
    val outputTable = args(4)
    
    try {
      // Initialize production Spark session
      val spark = SparkPlatform.spark
      
      // Production-specific operations can go here
      // (e.g., setting up Iceberg catalogs, registering UDFs, etc.)
      
      // Run the main ETL pipeline
      UserActivity.run(startDate, endDate, userEventsTable, purchasesTable, outputTable)
      
    } finally {
      SparkPlatform.stop()
    }
  }
}
```

### Core Job Logic
```scala
package com.example.useractivity

import org.apache.spark.sql.SparkSession
import com.example.sparkutils.{SparkPlatform, FetchUtils, WriteUtils}
import com.example.useractivity.model._
import com.example.useractivity.transformations.UserActivityTransformations

object UserActivity {
  def run(
    startDate: Int, 
    endDate: Int, 
    userEventsTable: String, 
    purchasesTable: String, 
    outputTable: String
  ): Unit = {
    implicit val spark: SparkSession = SparkPlatform.spark
    import spark.implicits._
    
    // Read source tables
    val userEvents = FetchUtils.readTableAsDataset[UserEvent](userEventsTable)
    val purchases = FetchUtils.readTableAsDataset[PurchaseTransaction](purchasesTable)
    val userProfiles = FetchUtils.readTableAsDataset[UserProfile]("lookup_user")
    
    // Apply transformations
    val filteredEvents = UserActivityTransformations.filterAndDedupeUserEvents(userEvents, startDate, endDate)
    val filteredPurchases = UserActivityTransformations.filterAndDedupePurchases(purchases, startDate, endDate)
    
    val enrichedEvents = UserActivityTransformations.enrichUserEventsWithProfiles(filteredEvents, userProfiles)
    val enrichedPurchases = UserActivityTransformations.enrichPurchasesWithProfiles(filteredPurchases, userProfiles)
    
    val userActivitySummary = UserActivityTransformations.aggregateUserActivity(enrichedEvents, enrichedPurchases)
    
    // Write output
    WriteUtils.writeDatasetToTable(userActivitySummary, outputTable)
  }
}
```

## 5. Test Infrastructure

### Test Base Setup
```scala
package com.example.test

import org.apache.spark.sql.{SparkSession, Dataset}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import com.holdenkarau.spark.testing.DatasetSuiteBase
import com.example.sparkutils.SparkPlatform

trait SparkTestBase extends AnyFunSuite with DatasetSuiteBase with BeforeAndAfterAll with BeforeAndAfterEach {
  
  override def beforeAll(): Unit = {
    super.beforeAll()
    // Set local mode for testing
    System.setProperty("SPARK_MODE", "local")
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
  def createTestDataset[T](data: Seq[T])(implicit spark: SparkSession, encoder: org.apache.spark.sql.Encoder[T]): Dataset[T] = {
    import spark.implicits._
    data.toDS()
  }
}
```

### Example Unit Test
```scala
package com.example.useractivity.transformations

import java.sql.Timestamp
import com.example.test.SparkTestBase
import com.example.useractivity.model._
import com.holdenkarau.spark.testing.DatasetSuiteBase

class UserActivityTransformationsTest extends SparkTestBase {
  
  test("filterAndDedupeUserEvents should filter by date range and deduplicate") {
    // Arrange
    val testEvents = Seq(
      UserEvent("1", "user1", "click", new Timestamp(1000), "session1", "/page1", "mobile", 20240101),
      UserEvent("2", "user1", "click", new Timestamp(2000), "session1", "/page2", "mobile", 20240101), // Latest for dedup
      UserEvent("3", "user2", "view", new Timestamp(1500), "session2", "/page1", "desktop", 20240102),
      UserEvent("4", "user3", "click", new Timestamp(1200), "session3", "/page3", "tablet", 20240105) // Outside date range
    )
    
    val inputDs = createTestDataset(testEvents)
    
    // Act
    val result = UserActivityTransformations.filterAndDedupeUserEvents(inputDs, 20240101, 20240102)
    val resultList = result.collect().toList
    
    // Assert
    assert(resultList.length == 2)
    
    val user1Event = resultList.find(_.user_id == "user1").get
    assert(user1Event.event_id == "2") // Should keep the latest event
    assert(user1Event.row_number == 1)
    
    val user2Event = resultList.find(_.user_id == "user2").get
    assert(user2Event.event_id == "3")
    assert(user2Event.row_number == 1)
  }
  
  test("enrichUserEventsWithProfiles should join with user profiles and filter active users") {
    // Arrange
    val filteredEvents = Seq(
      FilteredUserEvent("1", "user1", "click", new Timestamp(1000), "session1", "/page1", "mobile", 20240101, 1),
      FilteredUserEvent("2", "user2", "view", new Timestamp(1500), "session2", "/page2", "desktop", 20240101, 1),
      FilteredUserEvent("3", "user3", "click", new Timestamp(1200), "session3", "/page3", "tablet", 20240101, 1)
    )
    
    val userProfiles = Seq(
      UserProfile("user1", "alice", "alice@example.com", new Timestamp(500), "25-34", "US", "premium", true),
      UserProfile("user2", "bob", "bob@example.com", new Timestamp(600), "35-44", "UK", "basic", true),
      UserProfile("user3", "charlie", "charlie@example.com", new Timestamp(700), "18-24", "CA", "premium", false) // Inactive
    )
    
    val eventsDs = createTestDataset(filteredEvents)
    val profilesDs = createTestDataset(userProfiles)
    
    // Act
    val result = UserActivityTransformations.enrichUserEventsWithProfiles(eventsDs, profilesDs)
    val resultList = result.collect().toList
    
    // Assert
    assert(resultList.length == 2) // user3 filtered out because inactive
    
    val enrichedUser1 = resultList.find(_.user_id == "user1").get
    assert(enrichedUser1.username == "alice")
    assert(enrichedUser1.age_group == "25-34")
    assert(enrichedUser1.subscription_tier == "premium")
  }
}
```

### Example End-to-End Test
```scala
package com.example.useractivity

import java.sql.Timestamp
import com.example.test.SparkTestBase
import com.example.useractivity.model._
import com.example.sparkutils.{FetchUtils, WriteUtils}
import com.holdenkarau.spark.testing.DatasetSuiteBase

class UserActivityE2ETest extends SparkTestBase {
  
  test("complete ETL pipeline should process user activity correctly") {
    // Setup input data
    val userEvents = Seq(
      UserEvent("1", "user1", "click", new Timestamp(1000), "session1", "/page1", "mobile", 20240101),
      UserEvent("2", "user1", "view", new Timestamp(2000), "session1", "/page2", "mobile", 20240101),
      UserEvent("3", "user2", "click", new Timestamp(1500), "session2", "/page1", "desktop", 20240101)
    )
    
    val purchases = Seq(
      PurchaseTransaction("t1", "user1", "prod1", 99.99, new Timestamp(1500), "credit", "USD", false, 20240101),
      PurchaseTransaction("t2", "user2", "prod2", 149.99, new Timestamp(1600), "paypal", "USD", false, 20240101)
    )
    
    val userProfiles = Seq(
      UserProfile("user1", "alice", "alice@example.com", new Timestamp(500), "25-34", "US", "premium", true),
      UserProfile("user2", "bob", "bob@example.com", new Timestamp(600), "35-44", "UK", "basic", true)
    )
    
    // Write input tables to local storage
    WriteUtils.writeDatasetToTable(createTestDataset(userEvents), "user_events")
    WriteUtils.writeDatasetToTable(createTestDataset(purchases), "purchases") 
    WriteUtils.writeDatasetToTable(createTestDataset(userProfiles), "lookup_user")
    
    // Execute the job
    UserActivity.run(20240101, 20240101, "user_events", "purchases", "output_table")
    
    // Read and verify output
    val result = FetchUtils.readTableAsDataset[UserActivitySummary]("output_table")
    val resultList = result.collect().toList
    
    // Assertions
    assert(resultList.length == 2)
    
    val user1Summary = resultList.find(_.user_id == "user1").get
    assert(user1Summary.username == "alice")
    assert(user1Summary.total_events == 2)
    assert(user1Summary.unique_sessions == 1)
    assert(user1Summary.total_purchases == 1)
    assert(user1Summary.total_purchase_amount == 99.99)
    assert(user1Summary.most_common_device == "mobile")
    
    val user2Summary = resultList.find(_.user_id == "user2").get
    assert(user2Summary.username == "bob")
    assert(user2Summary.total_events == 1)
    assert(user2Summary.total_purchases == 1)
    assert(user2Summary.total_purchase_amount == 149.99)
  }
}
```

## 6. Key Design Considerations

### Type Safety Strategy
- Use typed Datasets throughout the pipeline for compile-time safety
- Define explicit case classes for each transformation stage
- Leverage Spark's Catalyst optimizer through typed operations

### Testing Philosophy
- **Unit Tests**: Test individual transformation functions with small, focused datasets
- **Integration Tests**: Test utility functions and platform abstraction
- **End-to-End Tests**: Test complete pipeline with realistic but minimal datasets

### Local vs Production Mode Strategy
- Environment variable `SPARK_MODE` controls execution mode
- In local mode, all table operations use in-memory storage via mutable map
- In production mode, operations use Iceberg tables with proper catalog configuration
- Test code automatically runs in local mode without code changes

### Error Handling Strategy
- Fail fast on missing required arguments
- Graceful handling of missing optional data (e.g., left joins with user profiles)
- Comprehensive logging for debugging production issues
- Clear error messages for common misconfigurations

### Performance Considerations
- Enable Spark's Adaptive Query Execution by default
- Use broadcast joins for small lookup tables
- Partition by date for time-series data