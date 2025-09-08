package com.example.sparkutils

import org.apache.spark.sql.{Dataset, DataFrame, SaveMode}

object WriteUtils {
  private def platform: SparkPlatformTrait = PlatformProvider.platform

  def writeDataFrameToTable(
      df: DataFrame,
      tableName: String,
      mode: SaveMode = SaveMode.Overwrite
  ): Unit =
    if (platform.isLocal) {
      platform.setLocalTable(tableName, df)
    } else {
      // In production, use standard Spark DataFrame API for now
      // (Iceberg integration can be enhanced later)
      df.write
        .mode(mode)
        .saveAsTable(tableName)
    }

  def writeDatasetToTable[T](
      ds: Dataset[T],
      tableName: String,
      mode: SaveMode = SaveMode.Overwrite
  ): Unit =
    if (platform.isLocal) {
      platform.setLocalTable(tableName, ds)
    } else {
      // In production, use standard Spark Dataset API for now
      ds.write
        .mode(mode)
        .saveAsTable(tableName)
    }
}
