package com.example.useractivity.model

import java.sql.Timestamp

// Base traits for shared field groups

/** Common user identifier trait */
trait UserIdTrait {
  def user_id: String
}

/** Common partitioning field trait */
trait PartitionDateTrait {
  def partition_date: Int // YYYYMMDD format
}

/** User profile information from the UserProfile lookup table */
trait UserProfileInfoTrait {
  def username: String
  def age_group: String
  def country: String
  def subscription_tier: String
}

/** Core event fields from UserEvent source table */
trait EventInfoTrait {
  def event_id: String
  def event_type: String
  def event_timestamp: Timestamp
  def session_id: String
  def page_url: String
  def device_type: String
}

/** Core transaction fields from PurchaseTransaction source table */
trait TransactionInfoTrait {
  def transaction_id: String
  def product_id: String
  def purchase_amount: Double
  def purchase_timestamp: Timestamp
  def payment_method: String
  def currency: String
  def is_refunded: Boolean
}

/** Additional user profile fields specific to UserProfile table */
trait UserProfileDetailsTrait {
  def email: String
  def registration_date: Timestamp
  def is_active: Boolean
}

/** Processing metadata added during data transformations */
trait ProcessingMetadataTrait {
  def row_number: Long
}

/** User summary metrics for aggregated data */
trait UserSummaryMetricsTrait {
  def total_events: Long
  def unique_sessions: Long
  def total_purchases: Long
  def total_purchase_amount: Double
  def avg_purchase_amount: Double
  def most_common_device: String
  def most_common_event_type: String
  def first_event_timestamp: Timestamp
  def last_event_timestamp: Timestamp
}
