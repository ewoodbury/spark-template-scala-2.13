package sparktoolbox

import org.apache.spark.sql.SparkSession
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

/**
 * Test-specific SparkPlatform implementation optimized for parallel test execution.
 * 
 * This implementation provides:
 * - Thread-safe operations for parallel test execution
 * - Isolated SparkSession per test thread to prevent interference
 * - Efficient resource sharing while maintaining test isolation
 * - Support for thousands of tests running across hundreds of parallel jobs
 * 
 * Key features:
 * - Each test thread gets its own SparkSession and local table storage
 * - Concurrent collections ensure thread safety
 * - Automatic cleanup and resource management
 * - Optimized for fast test execution with minimal overhead
 * 
 * This class should only be used in test environments and is loaded
 * dynamically by PlatformProvider when test context is detected.
 */
object TestSparkPlatform extends SparkPlatformTrait {
  
  // Thread-safe storage for multiple Spark sessions (one per test thread)
  private val _sparkSessions: ConcurrentHashMap[String, SparkSession] = new ConcurrentHashMap()
  
  // Thread-safe storage for local tables per test context
  private val _localTables: ConcurrentHashMap[String, ConcurrentHashMap[String, org.apache.spark.sql.Dataset[_]]] = 
    new ConcurrentHashMap()
  
  // Fallback main Spark session for non-test contexts
  private val _mainSpark: AtomicReference[Option[SparkSession]] = new AtomicReference(None)
  
  /**
   * Determines if the platform is running in local mode.
   * Checks system properties first (set by tests), then environment variables.
   */
  def isLocal: Boolean = {
    sys.props.getOrElse("SPARK_MODE", sys.env.getOrElse("SPARK_MODE", "production")) == "local"
  }
  
  /**
   * Gets the appropriate SparkSession for the current context.
   * - In test context: Returns isolated session per test thread
   * - Otherwise: Returns shared main session
   */
  def spark: SparkSession = {
    if (isLocal && isTestContext) {
      getOrCreateTestSparkSession()
    } else {
      getOrCreateMainSparkSession()
    }
  }
  
  /**
   * Determines if the current thread is running in a test context.
   */
  private def isTestContext: Boolean = {
    Thread.currentThread().getName.contains("ScalaTest") || 
    System.getProperty("sbt.testing", "false") == "true"
  }
  
  /**
   * Generates a unique context ID for the current test thread.
   * This ensures test isolation while allowing resource sharing within the same thread.
   */
  private def getTestContextId: String = {
    val threadName = Thread.currentThread().getName
    s"test-${threadName.hashCode.abs}"
  }
  
  /**
   * Creates or retrieves a SparkSession for the current test thread.
   * Each test thread gets its own isolated session to prevent interference.
   */
  private def getOrCreateTestSparkSession(): SparkSession = {
    val contextId = getTestContextId
    Option(_sparkSessions.get(contextId)) match {
      case Some(session) if !session.sparkContext.isStopped => session
      case _ =>
        val session = SparkSession.builder()
          .appName(s"Test Spark - $contextId")
          .master("local[*]")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
          .config("spark.ui.enabled", "false") // Disable UI for faster test execution
          .config("spark.sql.warehouse.dir", s"/tmp/spark-warehouse-$contextId")
          .config("spark.sql.shuffle.partitions", "4") // Smaller partition count for tests
          .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "8MB") // Smaller partitions for tests
          .getOrCreate()
        
        _sparkSessions.put(contextId, session)
        
        // Initialize local tables storage for this context
        _localTables.putIfAbsent(contextId, new ConcurrentHashMap())
        
        session
    }
  }
  
  /**
   * Creates or retrieves the main SparkSession for non-test contexts.
   */
  private def getOrCreateMainSparkSession(): SparkSession = {
    _mainSpark.get().getOrElse {
      val session = if (isLocal) {
        SparkSession.builder()
          .appName("Local Spark Application")
          .master("local[*]")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .getOrCreate()
      } else {
        SparkSession.builder()
          .appName("Production Spark Application")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
          .config("spark.sql.catalog.spark_catalog.type", "hive")
          .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
          .getOrCreate()
      }
      _mainSpark.compareAndSet(None, Some(session))
      _mainSpark.get().getOrElse(session)
    }
  }
  
  /**
   * Retrieves a locally stored table for the current context.
   */
  def getLocalTable(tableName: String): Option[org.apache.spark.sql.Dataset[_]] = {
    val contextId = if (isTestContext) getTestContextId else "main"
    Option(_localTables.get(contextId)).flatMap(tables => Option(tables.get(tableName)))
  }
  
  /**
   * Stores a table locally for the current context.
   */
  def setLocalTable(tableName: String, dataset: org.apache.spark.sql.Dataset[_]): Unit = {
    val contextId = if (isTestContext) getTestContextId else "main"
    val tables = _localTables.computeIfAbsent(contextId, _ => new ConcurrentHashMap())
    val _ = tables.put(tableName, dataset)
  }
  
  /**
   * Clears local tables for the current context.
   */
  def clearLocalTables(): Unit = {
    if (isTestContext) {
      val contextId = getTestContextId
      Option(_localTables.get(contextId)).foreach(_.clear())
    } else {
      // Clear main context tables
      Option(_localTables.get("main")).foreach(_.clear())
    }
  }
  
  /**
   * Cleans up all test contexts and their associated resources.
   * This method is useful for global cleanup after test suites complete.
   */
  def clearAllTestContexts(): Unit = {
    val _ = _localTables.entrySet().removeIf(entry => entry.getKey.startsWith("test-"))
    val sessionsToStop = new java.util.ArrayList[SparkSession]()
    _sparkSessions.entrySet().forEach { entry =>
      if (entry.getKey.startsWith("test-")) {
        val _ = sessionsToStop.add(entry.getValue)
      }
    }
    val _ = _sparkSessions.entrySet().removeIf(entry => entry.getKey.startsWith("test-"))
    
    // Stop sessions in separate loop to avoid concurrent modification
    sessionsToStop.forEach { session =>
      try {
        if (!session.sparkContext.isStopped) {
          session.stop()
        }
      } catch {
        case _: Exception => // Ignore errors during cleanup
      }
    }
  }
  
  /**
   * Stops the appropriate SparkSession based on context.
   * For test contexts, generally doesn't stop sessions to maintain performance.
   * For main context, stops the session and cleans up resources.
   */
  def stop(): Unit = {
    if (isTestContext) {
      // For test platform, we generally don't stop sessions during tests
      // to maintain performance, but we do clear local tables
      clearLocalTables()
    } else {
      // Stop main session
      _mainSpark.get().foreach { session =>
        try {
          if (!session.sparkContext.isStopped) {
            session.stop()
          }
        } catch {
          case _: Exception => // Ignore errors during cleanup
        }
      }
      _mainSpark.set(None)
      clearLocalTables()
    }
  }
  
  /**
   * Gets diagnostic information about the current test platform state.
   * Useful for debugging test issues.
   */
  def getDiagnosticInfo: Map[String, Any] = {
    Map(
      "isLocal" -> isLocal,
      "isTestContext" -> isTestContext,
      "testContextId" -> (if (isTestContext) getTestContextId else "N/A"),
      "activeTestSessions" -> _sparkSessions.size(),
      "activeContexts" -> _localTables.keySet().size(),
      "currentThread" -> Thread.currentThread().getName
    )
  }
}
