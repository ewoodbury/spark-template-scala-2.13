package com.example.sparkutils

import org.apache.spark.sql.SparkSession
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

object TestSparkPlatform extends SparkPlatformTrait {
  // Thread-safe storage for multiple Spark sessions (for testing)
  private val _sparkSessions: ConcurrentHashMap[String, SparkSession] = new ConcurrentHashMap()
  
  // Thread-safe storage for local tables per context
  private val _localTables: ConcurrentHashMap[String, ConcurrentHashMap[String, org.apache.spark.sql.Dataset[_]]] = 
    new ConcurrentHashMap()
  
  // Main production Spark session
  private val _mainSpark: AtomicReference[Option[SparkSession]] = new AtomicReference(None)
  
  def isLocal: Boolean = {
    // Check system properties first (for tests), then environment variables  
    sys.props.getOrElse("SPARK_MODE", sys.env.getOrElse("SPARK_MODE", "production")) == "local"
  }
  
  def spark: SparkSession = {
    if (isLocal && isTestContext) {
      getOrCreateTestSparkSession()
    } else {
      getOrCreateMainSparkSession()
    }
  }
  
  private def isTestContext: Boolean = {
    Thread.currentThread().getName.contains("ScalaTest") || 
    System.getProperty("sbt.testing", "false") == "true"
  }
  
  private def getTestContextId: String = {
    val threadName = Thread.currentThread().getName
    // Create a unique context per test thread
    s"test-${threadName.hashCode.abs}"
  }
  
  private def getOrCreateTestSparkSession(): SparkSession = {
    val contextId = getTestContextId
    Option(_sparkSessions.get(contextId)) match {
      case Some(session) if !session.sparkContext.isStopped => session
      case _ =>
        val session = SparkSession.builder()
          .appName(s"Test Spark Template - $contextId")
          .master("local[*]")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .config("spark.ui.enabled", "false") // Disable UI for tests
          .config("spark.sql.warehouse.dir", s"/tmp/spark-warehouse-$contextId")
          .getOrCreate()
        _sparkSessions.put(contextId, session)
        
        // Initialize local tables for this context
        _localTables.putIfAbsent(contextId, new ConcurrentHashMap())
        
        session
    }
  }
  
  private def getOrCreateMainSparkSession(): SparkSession = {
    _mainSpark.get().getOrElse {
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
      _mainSpark.compareAndSet(None, Some(session))
      _mainSpark.get().getOrElse(session)
    }
  }
  
  def getLocalTable(tableName: String): Option[org.apache.spark.sql.Dataset[_]] = {
    val contextId = if (isTestContext) getTestContextId else "main"
    Option(_localTables.get(contextId)).flatMap(tables => Option(tables.get(tableName)))
  }
  
  def setLocalTable(tableName: String, dataset: org.apache.spark.sql.Dataset[_]): Unit = {
    val contextId = if (isTestContext) getTestContextId else "main"
    val tables = _localTables.computeIfAbsent(contextId, _ => new ConcurrentHashMap())
    val _ = tables.put(tableName, dataset)
  }
  
  def clearLocalTables(): Unit = {
    if (isTestContext) {
      val contextId = getTestContextId
      Option(_localTables.get(contextId)).foreach(_.clear())
    } else {
      // Clear main context tables
      Option(_localTables.get("main")).foreach(_.clear())
    }
  }
  
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
  
  def stop(): Unit = {
    // For test platform, we generally don't stop sessions during tests
    // But if needed, this can clean up the current context
    if (isTestContext) {
      clearLocalTables()
    } else {
      _mainSpark.get().foreach { session =>
        if (!session.sparkContext.isStopped) {
          session.stop()
        }
      }
      _mainSpark.set(None)
    }
  }
}
