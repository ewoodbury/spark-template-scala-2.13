package com.example.useractivity.model

import java.sql.Timestamp

// Source Table 1: User Events
case class UserEvent(
    event_id: String,
    user_id: String,
    event_type: String,
    event_timestamp: Timestamp,
    session_id: String,
    page_url: String,
    device_type: String,
    partition_date: Int // YYYYMMDD format
)

// Source Table 2: Purchase Transactions
case class PurchaseTransaction(
    transaction_id: String,
    user_id: String,
    product_id: String,
    purchase_amount: Double,
    purchase_timestamp: Timestamp,
    payment_method: String,
    currency: String,
    is_refunded: Boolean,
    partition_date: Int // YYYYMMDD format
)

// Lookup Table: User Profiles
case class UserProfile(
    user_id: String,
    username: String,
    email: String,
    registration_date: Timestamp,
    age_group: String,
    country: String,
    subscription_tier: String,
    is_active: Boolean
)

// Intermediate Schema: Filtered User Events
case class FilteredUserEvent(
    event_id: String,
    user_id: String,
    event_type: String,
    event_timestamp: Timestamp,
    session_id: String,
    page_url: String,
    device_type: String,
    partition_date: Int,
    row_number: Long
)

// Intermediate Schema: Filtered Purchase Transactions
case class FilteredPurchaseTransaction(
    transaction_id: String,
    user_id: String,
    product_id: String,
    purchase_amount: Double,
    purchase_timestamp: Timestamp,
    payment_method: String,
    currency: String,
    is_refunded: Boolean,
    partition_date: Int,
    row_number: Long
)

// Intermediate Schema: Enriched User Events
case class EnrichedUserEvent(
    event_id: String,
    user_id: String,
    username: String,
    event_type: String,
    event_timestamp: Timestamp,
    session_id: String,
    page_url: String,
    device_type: String,
    age_group: String,
    country: String,
    subscription_tier: String,
    partition_date: Int
)

// Intermediate Schema: Enriched Purchase Transactions
case class EnrichedPurchaseTransaction(
    transaction_id: String,
    user_id: String,
    username: String,
    product_id: String,
    purchase_amount: Double,
    purchase_timestamp: Timestamp,
    payment_method: String,
    currency: String,
    is_refunded: Boolean,
    age_group: String,
    country: String,
    subscription_tier: String,
    partition_date: Int
)

// Final Output Schema: User Activity Summary
case class UserActivitySummary(
    user_id: String,
    username: String,
    age_group: String,
    country: String,
    subscription_tier: String,
    total_events: Long,
    unique_sessions: Long,
    total_purchases: Long,
    total_purchase_amount: Double,
    avg_purchase_amount: Double,
    most_common_device: String,
    most_common_event_type: String,
    first_event_timestamp: Timestamp,
    last_event_timestamp: Timestamp,
    partition_date: Int
)
