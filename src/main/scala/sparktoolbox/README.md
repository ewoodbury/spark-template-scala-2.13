# SparkUtils - Production-Grade Spark Utilities

A production-ready toolkit for Spark applications that aims to simplify Spark development and enable easy data pipeline testing.

## Key Features

### Testing as a Top Priority
- Clean setup of all Spark dependencies to run seamlessly in testing and production environments
- Parallel Test Execution: Thread-safe test utilities for fast CI/CD
- Isolated Environments: Each test thread gets its own SparkSession and in-memory table storage

### Production-Ready
- Type-Safe Operations: Compile-time guarantees with strongly-typed Dataset operations
- Comprehensive Error Handling: Detailed error messages and graceful failure recovery
- Thread-Safe: Concurrent operations with proper isolation

### Architecture
- Environment Detection: Automatic switching between local and production configurations
- Modular Design: Composable utilities that can be used independently
- Extensible: Easy to customize and extend for specific use cases


## Core Components

### SparkPlatform
Central Spark session management with environment-specific optimizations:

```scala
import sparkutils.SparkPlatform

// Automatically configured for your environment
val spark = SparkPlatform.spark

// Get session information
val info = SparkPlatform.getSessionInfo
```

Local Mode Features:
- `local[*]` execution with optimized settings
- Disabled Spark UI for cleaner development
- Smaller partition sizes for test data

Production Mode Features:
- Iceberg catalog integration
- Adaptive query execution optimizations
- Production-grade memory and shuffle settings
- Optimized file sizes and compression

### FetchUtils
Type-safe data reading with automatic environment detection:

```scala
import sparkutils.FetchUtils

// Read as DataFrame
val df = FetchUtils.readTableAsDataFrame("user_events")

// Read as strongly-typed Dataset
case class UserEvent(id: String, userId: String, event: String)
val events = FetchUtils.readTableAsDataset[UserEvent]("user_events")

// Read with column selection for performance
val subset = FetchUtils.readTableAsDataFrameWithColumns("user_events", Seq("id", "userId"))

// Check table existence
if (FetchUtils.tableExists("user_events")) {
  // Process table
}
```

### WriteUtils
Production-optimized data writing with multiple strategies:

```scala
import sparkutils.WriteUtils
import org.apache.spark.sql.SaveMode

// Basic write operations
WriteUtils.writeDataFrameToTable(df, "user_events")
WriteUtils.writeDatasetToTable(events, "user_events", SaveMode.Append)

// Partitioned writes for performance
WriteUtils.writeDataFrameToTablePartitioned(
  df, 
  "user_events", 
  Seq("year", "month", "day")
)

// Optimized writes for large-scale workloads
WriteUtils.writeDataFrameToTableOptimized(
  df, 
  "user_events",
  targetFileSizeMB = 256 // 256MB files for optimal performance
)
```

### PlatformProvider
Smart factory for automatic environment detection:

```scala
import sparkutils.PlatformProvider

// Automatically selects appropriate platform implementation
val platform = PlatformProvider.platform
val spark = platform.spark

// Get platform selection information
val info = PlatformProvider.getPlatformInfo
```

## Environment Modes

### Local Mode
Activated when `SPARK_MODE=local` environment variable is set.

Features:
- In-memory table storage for development and testing
- Optimized for single-machine execution
- Fast startup and teardown
- Perfect for unit tests and local development

Configuration:
```bash
export SPARK_MODE=local
```

### Production Mode
Default mode for cluster deployments.

Features:
- Iceberg table support with Hive metastore
- Production-optimized Spark configurations
- Large-scale performance optimizations
- Enterprise-grade reliability features

## Testing Support

SparkUtils includes a specialized test platform for parallel test execution:

```scala
import sparkutils.TestSparkPlatform

class MySparkTest extends SparkTestBase {
  "my test" should "process data correctly" in {
    // Each test gets isolated SparkSession and table storage
    val spark = TestSparkPlatform.spark
    // ... test implementation
  }
}
```

Test Features:
- Thread-Safe: Each test thread gets isolated resources
- Parallel Execution: Run hundreds of tests concurrently
- Fast: Optimized configurations for test workloads
- Clean: Automatic cleanup between tests

### Example Production Configuration

```scala
// Automatically applied in production mode
val spark = SparkPlatform.spark
// - spark.sql.adaptive.enabled = true
// - spark.sql.adaptive.coalescePartitions.enabled = true
// - spark.sql.adaptive.advisoryPartitionSizeInBytes = 128MB
// - compression = snappy
// - Iceberg integration enabled
```

## Usage Examples

### Basic ETL Pipeline

```scala
import sparkutils.{FetchUtils, WriteUtils, PlatformProvider}
import org.apache.spark.sql.functions._

object MyETLJob {
  def run(): Unit = {
    implicit val spark = PlatformProvider.platform.spark
    import spark.implicits._
    
    // Read source data
    val events = FetchUtils.readTableAsDataset[UserEvent]("raw_events")
    val users = FetchUtils.readTableAsDataset[User]("dim_users")
    
    // Transform data
    val enrichedEvents = events
      .join(users, "userId")
      .filter($"event_date" >= lit("2024-01-01"))
      .groupBy($"userId", $"event_date")
      .agg(
        count("*").as("event_count"),
        countDistinct("sessionId").as("session_count")
      )
    
    // Write results with partitioning for performance
    WriteUtils.writeDatasetToTablePartitioned(
      enrichedEvents,
      "analytics.user_daily_activity",
      Seq("event_date")
    )
  }
}
```

### Advanced Production Pipeline

```scala
import sparkutils.{FetchUtils, WriteUtils}
import org.apache.spark.sql.SaveMode

object LargeScaleProcessor {
  def processTerabytes(): Unit = {
    // Read from partitioned source
    val largeDataset = FetchUtils.readTableAsDataFrameWithColumns(
      "fact_events",
      Seq("user_id", "event_time", "event_type", "partition_date")
    )
    
    // Apply transformations with optimal partitioning
    val processed = largeDataset
      .repartition(col("partition_date")) // Optimize for downstream processing
      .cache() // Cache for multiple operations
      
    // Write with production optimizations
    WriteUtils.writeDataFrameToTableOptimized(
      processed,
      "processed_events",
      mode = SaveMode.Overwrite,
      targetFileSizeMB = 256 // Larger files for production workloads
    )
  }
}
```

## Making Changes

When extending SparkUtils:

1. Maintain Type Safety: Use strongly-typed operations where possible
2. Add Comprehensive Tests: Include both unit and integration tests
3. Document Thoroughly: Add scaladoc comments for all public APIs
4. Preserve Thread Safety: Ensure all operations are thread-safe
