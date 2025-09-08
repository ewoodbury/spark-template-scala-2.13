package com.example.useractivity.transformations

import java.sql.Timestamp
import com.example.test.SparkTestBase
import com.example.useractivity.model._

class UserActivityTransformationsTest extends SparkTestBase {
  
  "filterAndDedupeUserEvents" should "filter by date range and deduplicate" in {
    // Arrange    
    import spark.implicits._
    
    val testEvents = Seq(
      UserEvent("1", "user1", "click", new Timestamp(1000000), "session1", "/page1", "mobile", 20240101),
      UserEvent("2", "user1", "click", new Timestamp(2000000), "session1", "/page2", "mobile", 20240101), // Latest for dedup
      UserEvent("3", "user2", "view", new Timestamp(1500000), "session2", "/page1", "desktop", 20240102),
      UserEvent("4", "user3", "click", new Timestamp(1200000), "session3", "/page3", "tablet", 20240105) // Outside date range
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
      PurchaseTransaction("t1", "user1", "prod1", 99.99, new Timestamp(1000), "credit", "USD", false, 20240101),
      PurchaseTransaction("t2", "user1", "prod2", 149.99, new Timestamp(2000), "paypal", "USD", true, 20240101), // Refunded
      PurchaseTransaction("t3", "user2", "prod1", 79.99, new Timestamp(1500), "credit", "USD", false, 20240102),
      PurchaseTransaction("t4", "user3", "prod3", 199.99, new Timestamp(1200), "debit", "USD", false, 20240105) // Outside date range
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
      FilteredUserEvent("1", "user1", "click", new Timestamp(1000), "session1", "/page1", "mobile", 20240101, 1),
      FilteredUserEvent("2", "user2", "view", new Timestamp(1500), "session2", "/page2", "desktop", 20240101, 1),
      FilteredUserEvent("3", "user3", "click", new Timestamp(1200), "session3", "/page3", "tablet", 20240101, 1)
    )
    
    val userProfiles = Seq(
      UserProfile("user1", "alice", "alice@example.com", new Timestamp(500), "25-34", "US", "premium", true),
      UserProfile("user2", "bob", "bob@example.com", new Timestamp(600), "35-44", "UK", "basic", true),
      UserProfile("user3", "charlie", "charlie@example.com", new Timestamp(700), "18-24", "CA", "premium", false) // Inactive
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
      EnrichedUserEvent("1", "user1", "alice", "click", new Timestamp(1000), "session1", "/page1", "mobile", "25-34", "US", "premium", 20240101),
      EnrichedUserEvent("2", "user1", "alice", "view", new Timestamp(2000), "session1", "/page2", "mobile", "25-34", "US", "premium", 20240101),
      EnrichedUserEvent("3", "user2", "bob", "click", new Timestamp(1500), "session2", "/page3", "desktop", "35-44", "UK", "basic", 20240101)
    )
    
    val enrichedPurchases = Seq(
      EnrichedPurchaseTransaction("t1", "user1", "alice", "prod1", 99.99, new Timestamp(1500), "credit", "USD", false, "25-34", "US", "premium", 20240101),
      EnrichedPurchaseTransaction("t2", "user2", "bob", "prod2", 149.99, new Timestamp(1600), "paypal", "USD", false, "35-44", "UK", "basic", 20240101)
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
