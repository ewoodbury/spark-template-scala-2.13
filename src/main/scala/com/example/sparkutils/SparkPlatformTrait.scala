package com.example.sparkutils

import org.apache.spark.sql.SparkSession

trait SparkPlatformTrait {
  def spark: SparkSession
  def getLocalTable(tableName: String): Option[org.apache.spark.sql.Dataset[_]]
  def setLocalTable(tableName: String, dataset: org.apache.spark.sql.Dataset[_]): Unit
  def clearLocalTables(): Unit
  def isLocal: Boolean
  def stop(): Unit
}
