package com.example.sparkutils

import org.apache.spark.sql.{Dataset, DataFrame, Encoder}

object FetchUtils {
  private def platform: SparkPlatformTrait = PlatformProvider.platform

  def readTableAsDataFrame(tableName: String): DataFrame =
    if (platform.isLocal) {
      platform
        .getLocalTable(tableName)
        .getOrElse(throw new RuntimeException(s"Table $tableName not found in local storage"))
        .toDF()
    } else {
      platform.spark.table(tableName)
    }

  def readTableAsDataset[T](tableName: String)(implicit encoder: Encoder[T]): Dataset[T] =
    if (platform.isLocal) {
      platform
        .getLocalTable(tableName)
        .getOrElse(throw new RuntimeException(s"Table $tableName not found in local storage"))
        .as[T]
    } else {
      platform.spark.table(tableName).as[T]
    }
}
