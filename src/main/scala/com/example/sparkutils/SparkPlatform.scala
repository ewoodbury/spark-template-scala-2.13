package com.example.sparkutils

import org.apache.spark.sql.SparkSession
import scala.collection.mutable

object SparkPlatform extends SparkPlatformTrait {
  private var _spark: Option[SparkSession]                                       = None
  private val _localTables: mutable.Map[String, org.apache.spark.sql.Dataset[_]] = mutable.Map.empty

  def isLocal: Boolean = sys.env.getOrElse("SPARK_MODE", "production") == "local"

  def spark: SparkSession =
    _spark.getOrElse {
      val session = if (isLocal) {
        SparkSession
          .builder()
          .appName("Local Spark Template")
          .master("local[*]")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .getOrCreate()
      } else {
        SparkSession
          .builder()
          .appName("Production Spark Template")
          .config("spark.sql.adaptive.enabled", "true")
          .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
          .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
          .config("spark.sql.catalog.spark_catalog.type", "hive")
          .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
          )
          .getOrCreate()
      }
      _spark = Some(session)
      session
    }

  def getLocalTable(tableName: String): Option[org.apache.spark.sql.Dataset[_]] =
    _localTables.get(tableName)

  def setLocalTable(tableName: String, dataset: org.apache.spark.sql.Dataset[_]): Unit =
    _localTables(tableName) = dataset

  def clearLocalTables(): Unit = _localTables.clear()

  def stop(): Unit = {
    _spark.foreach(_.stop())
    _spark = None
    clearLocalTables()
  }
}
