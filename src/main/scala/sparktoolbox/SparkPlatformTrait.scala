package sparktoolbox

import org.apache.spark.sql.SparkSession

/**
 * Platform trait that defines the interface for Spark platform operations.
 * This abstraction allows for different implementations (production vs test environments)
 * while maintaining a consistent API.
 */
trait SparkPlatformTrait {
  /**
   * Get the SparkSession for this platform.
   * @return Active SparkSession instance
   */
  def spark: SparkSession
  
  /**
   * Retrieve a locally stored table/dataset by name.
   * Used primarily in local mode for in-memory table storage.
   * 
   * @param tableName Name of the table to retrieve
   * @return Some(Dataset) if found, None otherwise
   */
  def getLocalTable(tableName: String): Option[org.apache.spark.sql.Dataset[_]]
  
  /**
   * Store a dataset locally by name.
   * Used primarily in local mode for in-memory table storage.
   * 
   * @param tableName Name to store the table under
   * @param dataset Dataset to store
   */
  def setLocalTable(tableName: String, dataset: org.apache.spark.sql.Dataset[_]): Unit
  
  /**
   * Clear all locally stored tables.
   * Used for cleanup between test runs.
   */
  def clearLocalTables(): Unit
  
  /**
   * Check if the platform is running in local mode.
   * @return true if in local mode, false for production mode
   */
  def isLocal: Boolean
  
  /**
   * Stop the Spark session and clean up resources.
   * Should be called when shutting down the application.
   */
  def stop(): Unit
}
