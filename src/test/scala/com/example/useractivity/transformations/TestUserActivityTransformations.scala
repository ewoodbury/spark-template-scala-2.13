package com.example.useractivity.transformations

import java.sql.Timestamp
import com.example.test.SparkTestBase
import com.example.useractivity.model._

class TestUserActivityTransformations extends SparkTestBase {
  
  "filterAndDedupeUserEvents" should "filter by date range and deduplicate" in {
    // Arrange    
    import spark.implicits._
    
    val testEvents = Seq(
      UserEvent(
        event_id = "1",
        event_type = "click",
        event_timestamp = new Timestamp(1000000),
        session_id = "session1",
        page_url = "/page1",
        device_type = "mobile",
        user_id = "user1",
        partition_date = 20240101
      ),
      UserEvent(
        event_id = "2", 
        event_type = "click",
        event_timestamp = new Timestamp(2000000),
        session_id = "session1",
        page_url = "/page2",
        device_type = "mobile",
        user_id = "user1",
        partition_date = 20240101
      ), // Latest for dedup
      UserEvent(
        event_id = "3",
        event_type = "view",
        event_timestamp = new Timestamp(1500000),
        session_id = "session2",
        page_url = "/page1",
        device_type = "desktop",
        user_id = "user2",
        partition_date = 20240102
      ),
      UserEvent(
        event_id = "4",
        event_type = "click",
        event_timestamp = new Timestamp(1200000),
        session_id = "session3",
        page_url = "/page3",
        device_type = "tablet",
        user_id = "user3",
        partition_date = 20240105
      ) // Outside date range
    )
    
    val inputDs = createTestDataset(testEvents)
    
    // Act
    val result = UserActivityTransformations.filterAndDedupeUserEvents(inputDs, 20240101, 20240102)(spark)
    val resultList = result.collect().toList
    
    // Assert - We get all the filtered events, then we need to filter by row_number = 1 in the next step
    assert(resultList.length == 3) // All events within date range, dedup happens later
    
    val filteredResult = resultList.filter(_.row_number == 1)
    assert(filteredResult.length == 2)
    
    val user1Event = filteredResult.find(_.user_id == "user1").get
    assert(user1Event.event_id == "2") // Should keep the latest event
    
    val user2Event = filteredResult.find(_.user_id == "user2").get
    assert(user2Event.event_id == "3")
  }
  
  it should "filter by date range and exclude refunded purchases" in {
    // Arrange
    import spark.implicits._
    
    val testPurchases = Seq(
      PurchaseTransaction(
        transaction_id = "t1",
        product_id = "prod1",
        purchase_amount = 99.99,
        purchase_timestamp = new Timestamp(1000),
        payment_method = "credit",
        currency = "USD",
        is_refunded = false,
        user_id = "user1",
        partition_date = 20240101
      ),
      PurchaseTransaction(
        transaction_id = "t2",
        product_id = "prod2", 
        purchase_amount = 149.99,
        purchase_timestamp = new Timestamp(2000),
        payment_method = "paypal",
        currency = "USD",
        is_refunded = true,
        user_id = "user1",
        partition_date = 20240101
      ), // Refunded
      PurchaseTransaction(
        transaction_id = "t3",
        product_id = "prod1",
        purchase_amount = 79.99,
        purchase_timestamp = new Timestamp(1500),
        payment_method = "credit",
        currency = "USD",
        is_refunded = false,
        user_id = "user2",
        partition_date = 20240102
      ),
      PurchaseTransaction(
        transaction_id = "t4",
        product_id = "prod3",
        purchase_amount = 199.99,
        purchase_timestamp = new Timestamp(1200),
        payment_method = "debit",
        currency = "USD",
        is_refunded = false,
        user_id = "user3",
        partition_date = 20240105
      ) // Outside date range
    )
    
    val inputDs = createTestDataset(testPurchases)
    
    // Act
    val result = UserActivityTransformations.filterAndDedupePurchases(inputDs, 20240101, 20240102)(spark)
    val resultList = result.collect().toList
    
    // Assert
    assert(resultList.length == 2) // t2 excluded for refund, t4 excluded for date
    
    val user1Purchase = resultList.find(_.user_id == "user1").get
    assert(user1Purchase.transaction_id == "t1")
    assert(!user1Purchase.is_refunded)
    
    val user2Purchase = resultList.find(_.user_id == "user2").get
    assert(user2Purchase.transaction_id == "t3")
  }
  
  "enrichUserEventsWithProfiles" should "join with user profiles and filter active users" in {
    // Arrange
    import spark.implicits._
    
    val filteredEvents = Seq(
      FilteredUserEvent(
        event_id = "1",
        event_type = "click",
        event_timestamp = new Timestamp(1000),
        session_id = "session1",
        page_url = "/page1",
        device_type = "mobile",
        user_id = "user1",
        partition_date = 20240101,
        row_number = 1
      ),
      FilteredUserEvent(
        event_id = "2",
        event_type = "view",
        event_timestamp = new Timestamp(1500),
        session_id = "session2",
        page_url = "/page2",
        device_type = "desktop",
        user_id = "user2",
        partition_date = 20240101,
        row_number = 1
      ),
      FilteredUserEvent(
        event_id = "3",
        event_type = "click",
        event_timestamp = new Timestamp(1200),
        session_id = "session3",
        page_url = "/page3",
        device_type = "tablet",
        user_id = "user3",
        partition_date = 20240101,
        row_number = 1
      )
    )
    
    val userProfiles = Seq(
      UserProfile(
        user_id = "user1",
        username = "alice",
        age_group = "25-34",
        country = "US",
        subscription_tier = "premium",
        email = "alice@example.com",
        registration_date = new Timestamp(500),
        is_active = true
      ),
      UserProfile(
        user_id = "user2",
        username = "bob",
        age_group = "35-44",
        country = "UK",
        subscription_tier = "basic",
        email = "bob@example.com",
        registration_date = new Timestamp(600),
        is_active = true
      ),
      UserProfile(
        user_id = "user3",
        username = "charlie",
        age_group = "18-24",
        country = "CA",
        subscription_tier = "premium",
        email = "charlie@example.com",
        registration_date = new Timestamp(700),
        is_active = false
      ) // Inactive
    )
    
    val eventsDs = createTestDataset(filteredEvents)
    val profilesDs = createTestDataset(userProfiles)
    
    // Act
    val result = UserActivityTransformations.enrichUserEventsWithProfiles(eventsDs, profilesDs)(spark)
    val resultList = result.collect().toList
    
    // Assert
    assert(resultList.length == 3) // All events kept, but user3 will have "unknown" for missing profile fields
    
    val enrichedUser1 = resultList.find(_.user_id == "user1").get
    assert(enrichedUser1.username == "alice")
    assert(enrichedUser1.age_group == "25-34")
    assert(enrichedUser1.subscription_tier == "premium")
    
    val enrichedUser3 = resultList.find(_.user_id == "user3").get
    assert(enrichedUser3.username == "unknown") // Left join with inactive user
  }
  
  "aggregateUserActivity" should "combine events and purchases into summary" in {
    // Arrange
    import spark.implicits._
    
    val enrichedEvents = Seq(
      EnrichedUserEvent(
        event_id = "1",
        event_type = "click",
        event_timestamp = new Timestamp(1000),
        session_id = "session1",
        page_url = "/page1",
        device_type = "mobile",
        user_id = "user1",
        partition_date = 20240101,
        username = "alice",
        age_group = "25-34",
        country = "US",
        subscription_tier = "premium"
      ),
      EnrichedUserEvent(
        event_id = "2",
        event_type = "view",
        event_timestamp = new Timestamp(2000),
        session_id = "session1",
        page_url = "/page2",
        device_type = "mobile",
        user_id = "user1",
        partition_date = 20240101,
        username = "alice",
        age_group = "25-34",
        country = "US",
        subscription_tier = "premium"
      ),
      EnrichedUserEvent(
        event_id = "3",
        event_type = "click",
        event_timestamp = new Timestamp(1500),
        session_id = "session2",
        page_url = "/page3",
        device_type = "desktop",
        user_id = "user2",
        partition_date = 20240101,
        username = "bob",
        age_group = "35-44",
        country = "UK",
        subscription_tier = "basic"
      )
    )
    
    val enrichedPurchases = Seq(
      EnrichedPurchaseTransaction(
        transaction_id = "t1",
        product_id = "prod1",
        purchase_amount = 99.99,
        purchase_timestamp = new Timestamp(1500),
        payment_method = "credit",
        currency = "USD",
        is_refunded = false,
        user_id = "user1",
        partition_date = 20240101,
        username = "alice",
        age_group = "25-34",
        country = "US",
        subscription_tier = "premium"
      ),
      EnrichedPurchaseTransaction(
        transaction_id = "t2",
        product_id = "prod2",
        purchase_amount = 149.99,
        purchase_timestamp = new Timestamp(1600),
        payment_method = "paypal",
        currency = "USD",
        is_refunded = false,
        user_id = "user2",
        partition_date = 20240101,
        username = "bob",
        age_group = "35-44",
        country = "UK",
        subscription_tier = "basic"
      )
    )
    
    val eventsDs = createTestDataset(enrichedEvents)
    val purchasesDs = createTestDataset(enrichedPurchases)
    
    // Act
    val result = UserActivityTransformations.aggregateUserActivity(eventsDs, purchasesDs)(spark)
    val resultList = result.collect().toList
    
    // Assert
    assert(resultList.length == 2)
    
    val user1Summary = resultList.find(_.user_id == "user1").get
    assert(user1Summary.username == "alice")
    assert(user1Summary.total_events == 2)
    assert(user1Summary.unique_sessions == 1)
    assert(user1Summary.total_purchases == 1)
    assert(user1Summary.total_purchase_amount == 99.99)
    assert(user1Summary.most_common_device == "mobile")
    
    val user2Summary = resultList.find(_.user_id == "user2").get
    assert(user2Summary.username == "bob")
    assert(user2Summary.total_events == 1)
    assert(user2Summary.total_purchases == 1)
    assert(user2Summary.total_purchase_amount == 149.99)
  }
}
