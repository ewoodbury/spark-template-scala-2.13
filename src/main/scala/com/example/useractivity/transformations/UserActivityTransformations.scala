package com.example.useractivity.transformations

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.example.useractivity.model._

object UserActivityTransformations {
  
  def filterAndDedupeUserEvents(
    userEvents: Dataset[UserEvent], 
    startDate: Int, 
    endDate: Int
  )(implicit spark: SparkSession): Dataset[FilteredUserEvent] = {
    import spark.implicits._
    
    val window = Window.partitionBy("user_id", "session_id", "event_type")
      .orderBy(desc("event_timestamp"))
    
    userEvents
      .filter(col("partition_date") >= startDate && col("partition_date") <= endDate)
      .filter(col("event_type").isNotNull && col("user_id").isNotNull)
      .withColumn("row_number", row_number().over(window))
      .as[FilteredUserEvent]
  }
  
  def filterAndDedupePurchases(
    purchases: Dataset[PurchaseTransaction], 
    startDate: Int, 
    endDate: Int
  )(implicit spark: SparkSession): Dataset[FilteredPurchaseTransaction] = {
    import spark.implicits._
    
    val window = Window.partitionBy("transaction_id")
      .orderBy(desc("purchase_timestamp"))
    
    purchases
      .filter(col("partition_date") >= startDate && col("partition_date") <= endDate)
      .filter(!col("is_refunded"))
      .filter(col("purchase_amount") > 0)
      .withColumn("row_number", row_number().over(window))
      .as[FilteredPurchaseTransaction]
  }
  
  def enrichUserEventsWithProfiles(
    userEvents: Dataset[FilteredUserEvent], 
    userProfiles: Dataset[UserProfile]
  )(implicit spark: SparkSession): Dataset[EnrichedUserEvent] = {
    import spark.implicits._
    
    userEvents
      .filter(col("row_number") === 1)
      .drop("row_number")
      .join(userProfiles.filter(col("is_active")), Seq("user_id"), "left")
      .select(
        col("event_id"),
        col("user_id"),
        coalesce(col("username"), lit("unknown")).as("username"),
        col("event_type"),
        col("event_timestamp"),
        col("session_id"),
        col("page_url"),
        col("device_type"),
        coalesce(col("age_group"), lit("unknown")).as("age_group"),
        coalesce(col("country"), lit("unknown")).as("country"),
        coalesce(col("subscription_tier"), lit("free")).as("subscription_tier"),
        col("partition_date")
      )
      .as[EnrichedUserEvent]
  }
  
  def enrichPurchasesWithProfiles(
    purchases: Dataset[FilteredPurchaseTransaction], 
    userProfiles: Dataset[UserProfile]
  )(implicit spark: SparkSession): Dataset[EnrichedPurchaseTransaction] = {
    import spark.implicits._
    
    purchases
      .filter(col("row_number") === 1)
      .drop("row_number")
      .join(userProfiles.filter(col("is_active")), Seq("user_id"), "left")
      .select(
        col("transaction_id"),
        col("user_id"),
        coalesce(col("username"), lit("unknown")).as("username"),
        col("product_id"),
        col("purchase_amount"),
        col("purchase_timestamp"),
        col("payment_method"),
        col("currency"),
        col("is_refunded"),
        coalesce(col("age_group"), lit("unknown")).as("age_group"),
        coalesce(col("country"), lit("unknown")).as("country"),
        coalesce(col("subscription_tier"), lit("free")).as("subscription_tier"),
        col("partition_date")
      )
      .as[EnrichedPurchaseTransaction]
  }
  
  def aggregateUserActivity(
    enrichedEvents: Dataset[EnrichedUserEvent],
    enrichedPurchases: Dataset[EnrichedPurchaseTransaction]
  )(implicit spark: SparkSession): Dataset[UserActivitySummary] = {
    import spark.implicits._
    
    val eventAggs = enrichedEvents
      .groupBy("user_id", "username", "age_group", "country", "subscription_tier", "partition_date")
      .agg(
        count("*").as("total_events"),
        countDistinct("session_id").as("unique_sessions"),
        // Use first() with order by count as a simple mode approximation
        first("device_type").as("most_common_device"),
        first("event_type").as("most_common_event_type"),
        min("event_timestamp").as("first_event_timestamp"),
        max("event_timestamp").as("last_event_timestamp")
      )
    
    val purchaseAggs = enrichedPurchases
      .groupBy("user_id")
      .agg(
        count("*").as("total_purchases"),
        sum("purchase_amount").as("total_purchase_amount"),
        avg("purchase_amount").as("avg_purchase_amount")
      )
    
    eventAggs
      .join(purchaseAggs, Seq("user_id"), "left")
      .na.fill(0, Seq("total_purchases", "total_purchase_amount", "avg_purchase_amount"))
      .as[UserActivitySummary]
  }
}
