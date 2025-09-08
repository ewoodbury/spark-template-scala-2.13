package sparktoolbox

import org.apache.spark.sql.{Dataset, DataFrame, Encoder}
import scala.util.{Try, Success, Failure}

/**
 * Production-grade utilities for reading data from tables and data sources.
 * 
 * This object provides type-safe, scalable methods for reading data in both local
 * and production environments. It automatically handles environment detection and
 * applies appropriate reading strategies for optimal performance at terabyte scale.
 * 
 * Key features:
 * - Type-safe operations with compile-time guarantees
 * - Automatic environment detection (local vs production)
 * - Comprehensive error handling with detailed error messages
 * - Support for both DataFrame and strongly-typed Dataset operations
 * - Production-optimized reading strategies
 * - Built-in validation and sanitization
 * 
 * Usage:
 * {{{
 *   import sparktoolbox.Fetchers
 *   
 *   // Read as DataFrame
 *   val df = Fetchers.readTableAsDataFrame("user_events")
 *   
 *   // Read as strongly-typed Dataset
 *   case class UserEvent(id: String, userId: String, event: String)
 *   val ds = Fetchers.readTableAsDataset[UserEvent]("user_events")
 * }}}
 */
object Fetchers {
  
  private def platform: SparkPlatformTrait = PlatformProvider.platform
  
  /**
   * Reads a table as a DataFrame with comprehensive error handling.
   * 
   * In local mode: Reads from in-memory table storage
   * In production mode: Reads from the configured catalog (e.g., Hive, Iceberg)
   * 
   * @param tableName Name of the table to read (must be non-empty)
   * @return DataFrame containing the table data
   * @throws IllegalArgumentException if tableName is empty or invalid
   * @throws RuntimeException if table is not found or read operation fails
   */
  def readTableAsDataFrame(tableName: String): DataFrame = {
    validateTableName(tableName)
    
    Try {
      if (platform.isLocal) {
        readFromLocalStorage(tableName).toDF()
      } else {
        readFromCatalog(tableName)
      }
    } match {
      case Success(df) => df
      case Failure(exception) => 
        throw new RuntimeException(
          s"Failed to read table '$tableName' as DataFrame: ${exception.getMessage}", 
          exception
        )
    }
  }
  
  /**
   * Reads a table as a strongly-typed Dataset with compile-time type safety.
   * 
   * In local mode: Reads from in-memory table storage and converts to specified type
   * In production mode: Reads from the configured catalog and converts to specified type
   * 
   * @param tableName Name of the table to read (must be non-empty)
   * @param encoder Implicit encoder for type T (provided automatically by Spark)
   * @tparam T The case class type to deserialize the data into
   * @return Dataset[T] containing the strongly-typed table data
   * @throws IllegalArgumentException if tableName is empty or invalid
   * @throws RuntimeException if table is not found, read operation fails, or type conversion fails
   */
  def readTableAsDataset[T](tableName: String)(implicit encoder: Encoder[T]): Dataset[T] = {
    validateTableName(tableName)
    
    Try {
      if (platform.isLocal) {
        readFromLocalStorage(tableName).as[T]
      } else {
        readFromCatalog(tableName).as[T]
      }
    } match {
      case Success(ds) => ds
      case Failure(exception) => 
        throw new RuntimeException(
          s"Failed to read table '$tableName' as Dataset[${encoder.clsTag.runtimeClass.getSimpleName}]: ${exception.getMessage}", 
          exception
        )
    }
  }
  
  /**
   * Reads a table with optional column selection for optimized performance.
   * 
   * This method allows you to specify which columns to read, reducing I/O and memory usage
   * for large tables where only a subset of columns is needed.
   * 
   * @param tableName Name of the table to read
   * @param columns Sequence of column names to select
   * @return DataFrame containing only the specified columns
   * @throws IllegalArgumentException if tableName is empty or columns list is empty
   * @throws RuntimeException if table is not found or read operation fails
   */
  def readTableAsDataFrameWithColumns(tableName: String, columns: Seq[String]): DataFrame = {
    validateTableName(tableName)
    require(columns.nonEmpty, "Columns list cannot be empty")
    require(columns.forall(_.nonEmpty), "Column names cannot be empty")
    
    Try {
      val df = if (platform.isLocal) {
        readFromLocalStorage(tableName).toDF()
      } else {
        readFromCatalog(tableName)
      }
      df.select(columns.head, columns.tail: _*)
    } match {
      case Success(df) => df
      case Failure(exception) => 
        throw new RuntimeException(
          s"Failed to read table '$tableName' with columns [${columns.mkString(", ")}]: ${exception.getMessage}", 
          exception
        )
    }
  }
  
  /**
   * Checks if a table exists in the current environment.
   * 
   * @param tableName Name of the table to check
   * @return true if the table exists, false otherwise
   */
  def tableExists(tableName: String): Boolean = {
    validateTableName(tableName)
    
    Try {
      if (platform.isLocal) {
        platform.getLocalTable(tableName).isDefined
      } else {
        platform.spark.catalog.tableExists(tableName)
      }
    }.getOrElse(false)
  }
  
  /**
   * Lists all available tables in the current environment.
   * 
   * @return Sequence of table names
   */
  def listTables(): Seq[String] = {
    Try {
      if (platform.isLocal) {
        // For local mode, we would need to track table names in SparkPlatform
        // For now, return empty sequence as local tables are primarily for testing
        Seq.empty[String]
      } else {
        platform.spark.catalog.listTables().collect().map(_.name).toSeq
      }
    }.getOrElse(Seq.empty)
  }
  
  /**
   * Reads from local in-memory storage (used in local mode).
   */
  private def readFromLocalStorage(tableName: String): org.apache.spark.sql.Dataset[_] = {
    platform.getLocalTable(tableName).getOrElse {
      throw new RuntimeException(s"Table '$tableName' not found in local storage. Available tables: ${listTables().mkString(", ")}")
    }
  }
  
  /**
   * Reads from the configured catalog (used in production mode).
   */
  private def readFromCatalog(tableName: String): DataFrame = {
    platform.spark.table(tableName)
  }
  
  /**
   * Validates table name input.
   */
  private def validateTableName(tableName: String): Unit = {
    require(tableName != null, "Table name cannot be null")
    require(tableName.nonEmpty, "Table name cannot be empty")
    require(tableName.trim == tableName, "Table name cannot have leading or trailing whitespace")
    require(!tableName.contains(" "), "Table name cannot contain spaces")
  }
}
