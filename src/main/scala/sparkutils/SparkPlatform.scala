package sparkutils

import org.apache.spark.sql.SparkSession
import scala.collection.concurrent.TrieMap
import scala.util.{Try, Success, Failure}

/**
 * Production-grade Spark platform for managing SparkSession lifecycle and table operations.
 * 
 * This object provides a centralized way to manage Spark sessions with support for both
 * local development and production environments. It automatically detects the environment
 * and applies appropriate configurations for optimal performance at scale.
 * 
 * Key features:
 * - Thread-safe operations using concurrent collections
 * - Automatic environment detection (local vs production)
 * - Production-optimized Spark configurations
 * - Built-in support for Iceberg in production environments
 * - Local table storage for development/testing
 * - Comprehensive error handling and logging
 * 
 * Usage:
 * {{{
 *   import sparkutils.SparkPlatform
 *   
 *   val spark = SparkPlatform.spark
 *   // Use spark session...
 *   
 *   // Always call stop() when shutting down
 *   SparkPlatform.stop()
 * }}}
 */
object SparkPlatform extends SparkPlatformTrait {
  
  @volatile private var _spark: Option[SparkSession] = None
  private val _localTables: TrieMap[String, org.apache.spark.sql.Dataset[_]] = TrieMap.empty
  
  /**
   * Determines if the platform is running in local mode.
   * Local mode is enabled when SPARK_MODE environment variable is set to "local".
   * 
   * @return true if in local mode, false for production mode
   */
  def isLocal: Boolean = sys.env.getOrElse("SPARK_MODE", "production") == "local"
  
  /**
   * Gets or creates the SparkSession with appropriate configurations for the environment.
   * 
   * Local mode configuration:
   * - Uses local[*] master
   * - Enables adaptive query execution
   * - Optimized for development and testing
   * 
   * Production mode configuration:
   * - Uses cluster-submitted configurations
   * - Includes Iceberg catalog setup
   * - Optimized for large-scale data processing
   * 
   * @return Thread-safe SparkSession instance
   * @throws RuntimeException if SparkSession creation fails
   */
  def spark: SparkSession = {
    _spark.getOrElse {
      synchronized {
        _spark.getOrElse {
          val session = createSparkSession() match {
            case Success(s) => s
            case Failure(exception) => 
              throw new RuntimeException(s"Failed to create SparkSession: ${exception.getMessage}", exception)
          }
          _spark = Some(session)
          session
        }
      }
    }
  }
  
  /**
   * Creates a new SparkSession with environment-appropriate configurations.
   * 
   * @return Try[SparkSession] containing either the created session or the failure
   */
  private def createSparkSession(): Try[SparkSession] = Try {
    if (isLocal) {
      createLocalSparkSession()
    } else {
      createProductionSparkSession()
    }
  }
  
  /**
   * Creates a SparkSession optimized for local development.
   */
  private def createLocalSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("Local Spark Application")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .config("spark.ui.enabled", "false") // Disable UI for cleaner local development
      .getOrCreate()
  }
  
  /**
   * Creates a SparkSession optimized for production environments.
   * Includes Iceberg configuration for production data lake operations.
   */
  private def createProductionSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("Production Spark Application")
      // Production-grade adaptive query execution settings
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
      
      // Iceberg configuration for production data lake
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      
      // Performance optimizations for large-scale processing
      .config("spark.sql.execution.arrow.pyspark.enabled", "true")
      .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.sql.parquet.mergeSchema", "false")
      
      // Memory and shuffle optimizations
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.sql.files.maxPartitionBytes", "128MB")
      .config("spark.sql.files.openCostInBytes", "4MB")
      
      .getOrCreate()
  }
  
  /**
   * Retrieves a locally stored table/dataset by name.
   * Used primarily in local mode for in-memory table storage during development and testing.
   * 
   * @param tableName Name of the table to retrieve
   * @return Some(Dataset) if the table exists, None otherwise
   */
  def getLocalTable(tableName: String): Option[org.apache.spark.sql.Dataset[_]] = {
    require(tableName.nonEmpty, "Table name cannot be empty")
    _localTables.get(tableName)
  }
  
  /**
   * Stores a dataset locally by name.
   * Used primarily in local mode for in-memory table storage during development and testing.
   * 
   * @param tableName Name to store the table under
   * @param dataset Dataset to store
   * @throws IllegalArgumentException if tableName is empty
   */
  def setLocalTable(tableName: String, dataset: org.apache.spark.sql.Dataset[_]): Unit = {
    require(tableName.nonEmpty, "Table name cannot be empty")
    require(dataset != null, "Dataset cannot be null")
    val _ = _localTables.put(tableName, dataset)
  }
  
  /**
   * Clears all locally stored tables.
   * Used for cleanup between test runs or when resetting the local environment.
   */
  def clearLocalTables(): Unit = {
    _localTables.clear()
  }
  
  /**
   * Stops the Spark session and cleans up all resources.
   * 
   * This method should be called when shutting down the application to ensure
   * proper cleanup of resources and prevent resource leaks.
   * 
   * It's safe to call this method multiple times.
   */
  def stop(): Unit = {
    synchronized {
      _spark.foreach { session =>
        Try(session.stop()).recover {
          case ex => 
            System.err.println(s"Warning: Error stopping SparkSession: ${ex.getMessage}")
        }
      }
      _spark = None
      clearLocalTables()
    }
  }
  
  /**
   * Gets basic information about the current Spark session.
   * Useful for debugging and monitoring.
   * 
   * @return Map containing session information
   */
  def getSessionInfo: Map[String, String] = {
    _spark.map { session =>
      Map(
        "applicationId" -> session.sparkContext.applicationId,
        "applicationName" -> session.sparkContext.appName,
        "master" -> session.sparkContext.master,
        "version" -> session.version,
        "isLocal" -> isLocal.toString
      )
    }.getOrElse(Map("status" -> "No active SparkSession"))
  }
}
