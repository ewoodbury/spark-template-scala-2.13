package com.example.useractivity

import java.sql.Timestamp
import com.example.test.SparkTestBase
import com.example.useractivity.model._
import sparktoolbox.{Writers, Fetchers}

class TestE2EUserActivity extends SparkTestBase {
  
  "complete ETL pipeline" should "process user activity correctly" in {
    import spark.implicits._
    
    // Setup input data
    val userEvents = Seq(
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
        event_type = "view",
        event_timestamp = new Timestamp(2000000), 
        session_id = "session1",
        page_url = "/page2",
        device_type = "mobile",
        user_id = "user1",
        partition_date = 20240101
      ),
      UserEvent(
        event_id = "3",
        event_type = "click",
        event_timestamp = new Timestamp(1500000),
        session_id = "session2", 
        page_url = "/page1",
        device_type = "desktop",
        user_id = "user2",
        partition_date = 20240101
      )
    )
    
    val purchases = Seq(
      PurchaseTransaction(
        transaction_id = "t1",
        product_id = "prod1",
        purchase_amount = 99.99,
        purchase_timestamp = new Timestamp(1500000),
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
        purchase_timestamp = new Timestamp(1600000),
        payment_method = "paypal",
        currency = "USD",
        is_refunded = false,
        user_id = "user2",
        partition_date = 20240101
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
        registration_date = new Timestamp(500000),
        is_active = true
      ),
      UserProfile(
        user_id = "user2",
        username = "bob",
        age_group = "35-44",
        country = "UK",
        subscription_tier = "basic",
        email = "bob@example.com",
        registration_date = new Timestamp(600000),
        is_active = true
      )
    )
    
    // Write input tables to local storage
    Writers.writeDatasetToTable(createTestDataset(userEvents), "user_events")
    Writers.writeDatasetToTable(createTestDataset(purchases), "purchases") 
    Writers.writeDatasetToTable(createTestDataset(userProfiles), "lookup_user")
    
    // Execute the job
    UserActivity.run(
      startDate=20240101, 
      endDate=20240101, 
      userEventsTable="user_events", 
      purchasesTable="purchases", 
      outputTable="output_table",
    )
    
    // Read and verify output
    val result = Fetchers.readTableAsDataset[UserActivitySummary]("output_table")
    val resultList = result.collect().toList
    
    // Assertions
    assert(resultList.length == 2)
    
    val user1Summary = resultList.find(_.user_id == "user1").get
    assert(user1Summary.username == "alice")
    assert(user1Summary.total_events == 2)
    assert(user1Summary.unique_sessions == 1)
    assert(user1Summary.total_purchases == 1)
    assert(user1Summary.total_purchase_amount == 99.99)
    assert(user1Summary.most_common_device == "mobile")
    assert(user1Summary.age_group == "25-34")
    assert(user1Summary.subscription_tier == "premium")
    
    val user2Summary = resultList.find(_.user_id == "user2").get
    assert(user2Summary.username == "bob")
    assert(user2Summary.total_events == 1)
    assert(user2Summary.total_purchases == 1)
    assert(user2Summary.total_purchase_amount == 149.99)
    assert(user2Summary.most_common_device == "desktop")
    assert(user2Summary.age_group == "35-44")
    assert(user2Summary.subscription_tier == "basic")
  }
}
