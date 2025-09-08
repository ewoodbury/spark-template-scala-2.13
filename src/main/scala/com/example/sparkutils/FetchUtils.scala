package com.example.sparkutils

import org.apache.spark.sql.{Dataset, DataFrame, Encoder}

object FetchUtils {
  import SparkPlatform.spark
  
  def readTableAsDataFrame(tableName: String): DataFrame = {
    if (SparkPlatform.isLocal) {
      SparkPlatform.getLocalTable(tableName)
        .getOrElse(throw new RuntimeException(s"Table $tableName not found in local storage"))
        .toDF()
    } else {
      spark.table(tableName)
    }
  }
  
  def readTableAsDataset[T](tableName: String)(implicit encoder: Encoder[T]): Dataset[T] = {
    if (SparkPlatform.isLocal) {
      SparkPlatform.getLocalTable(tableName)
        .getOrElse(throw new RuntimeException(s"Table $tableName not found in local storage"))
        .as[T]
    } else {
      spark.table(tableName).as[T]
    }
  }
}
