package com.example.useractivity.model

import java.sql.Timestamp

// Source Table 1: User Events
case class UserEvent(
    // EventInfoTrait
    event_id: String,
    event_type: String,
    event_timestamp: Timestamp,
    session_id: String,
    page_url: String,
    device_type: String,
    // UserIdTrait
    user_id: String,
    // PartitionDateTrait
    partition_date: Int, // YYYYMMDD format
) extends EventInfoTrait
    with UserIdTrait
    with PartitionDateTrait

// Source Table 2: Purchase Transactions
case class PurchaseTransaction(
    // TransactionInfoTrait
    transaction_id: String,
    product_id: String,
    purchase_amount: Double,
    purchase_timestamp: Timestamp,
    payment_method: String,
    currency: String,
    is_refunded: Boolean,
    // UserIdTrait
    user_id: String,
    // PartitionDateTrait
    partition_date: Int, // YYYYMMDD format
) extends TransactionInfoTrait
    with UserIdTrait
    with PartitionDateTrait

// Lookup Table: User Profiles
case class UserProfile(
    // UserIdTrait
    user_id: String,
    // UserProfileInfoTrait
    username: String,
    age_group: String,
    country: String,
    subscription_tier: String,
    // UserProfileDetailsTrait
    email: String,
    registration_date: Timestamp,
    is_active: Boolean,
) extends UserIdTrait
    with UserProfileInfoTrait
    with UserProfileDetailsTrait

// Intermediate Schema: Filtered User Events
case class FilteredUserEvent(
    // EventInfoTrait (from UserEvent)
    event_id: String,
    event_type: String,
    event_timestamp: Timestamp,
    session_id: String,
    page_url: String,
    device_type: String,
    // UserIdTrait (from UserEvent)
    user_id: String,
    // PartitionDateTrait (from UserEvent)
    partition_date: Int,
    // ProcessingMetadataTrait (added in filtering step)
    row_number: Long,
) extends EventInfoTrait
    with UserIdTrait
    with PartitionDateTrait
    with ProcessingMetadataTrait

// Intermediate Schema: Filtered Purchase Transactions
case class FilteredPurchaseTransaction(
    // TransactionInfoTrait
    transaction_id: String,
    product_id: String,
    purchase_amount: Double,
    purchase_timestamp: Timestamp,
    payment_method: String,
    currency: String,
    is_refunded: Boolean,
    // UserIdTrait
    user_id: String,
    // PartitionDateTrai
    partition_date: Int,
    // ProcessingMetadataTrai
    row_number: Long,
) extends TransactionInfoTrait
    with UserIdTrait
    with PartitionDateTrait
    with ProcessingMetadataTrait

// Intermediate Schema: Enriched User Events
case class EnrichedUserEvent(
    // EventInfoTrait
    event_id: String,
    event_type: String,
    event_timestamp: Timestamp,
    session_id: String,
    page_url: String,
    device_type: String,
    // UserIdTrait (from UserEvent)
    user_id: String,
    // PartitionDateTrait (from UserEvent)
    partition_date: Int,
    // UserProfileInfoTrait (enriched from UserProfile lookup)
    username: String,
    age_group: String,
    country: String,
    subscription_tier: String,
) extends EventInfoTrait
    with UserIdTrait
    with PartitionDateTrait
    with UserProfileInfoTrait

// Intermediate Schema: Enriched Purchase Transactions
case class EnrichedPurchaseTransaction(
    // TransactionInfoTrait
    transaction_id: String,
    product_id: String,
    purchase_amount: Double,
    purchase_timestamp: Timestamp,
    payment_method: String,
    currency: String,
    is_refunded: Boolean,
    // UserIdTrait
    user_id: String,
    // PartitionDateTrait
    partition_date: Int,
    // UserProfileInfoTrait
    username: String,
    age_group: String,
    country: String,
    subscription_tier: String,
) extends TransactionInfoTrait
    with UserIdTrait
    with PartitionDateTrait
    with UserProfileInfoTrait

// Final Output Schema: User Activity Summary
case class UserActivitySummary(
    // UserIdTrait
    user_id: String,
    // PartitionDateTrait
    partition_date: Int,
    // UserProfileInfoTrai
    username: String,
    age_group: String,
    country: String,
    subscription_tier: String,
    // UserSummaryMetricsTrait
    total_events: Long,
    unique_sessions: Long,
    total_purchases: Long,
    total_purchase_amount: Double,
    avg_purchase_amount: Double,
    most_common_device: String,
    most_common_event_type: String,
    first_event_timestamp: Timestamp,
    last_event_timestamp: Timestamp,
) extends UserIdTrait
    with PartitionDateTrait
    with UserProfileInfoTrait
    with UserSummaryMetricsTrait
