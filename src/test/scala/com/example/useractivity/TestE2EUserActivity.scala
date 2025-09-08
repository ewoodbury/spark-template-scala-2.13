package com.example.useractivity

import java.sql.Timestamp
import com.example.test.SparkTestBase
import com.example.useractivity.model._
import com.example.sparkutils.{WriteUtils, FetchUtils}

class TestE2EUserActivity extends SparkTestBase {
  
  "complete ETL pipeline" should "process user activity correctly" in {
    import spark.implicits._
    
    // Setup input data
    val userEvents = Seq(
      UserEvent("1", "user1", "click", new Timestamp(1000000), "session1", "/page1", "mobile", 20240101),
      UserEvent("2", "user1", "view", new Timestamp(2000000), "session1", "/page2", "mobile", 20240101),
      UserEvent("3", "user2", "click", new Timestamp(1500000), "session2", "/page1", "desktop", 20240101)
    )
    
    val purchases = Seq(
      PurchaseTransaction("t1", "user1", "prod1", 99.99, new Timestamp(1500000), "credit", "USD", false, 20240101),
      PurchaseTransaction("t2", "user2", "prod2", 149.99, new Timestamp(1600000), "paypal", "USD", false, 20240101)
    )
    
    val userProfiles = Seq(
      UserProfile("user1", "alice", "alice@example.com", new Timestamp(500000), "25-34", "US", "premium", true),
      UserProfile("user2", "bob", "bob@example.com", new Timestamp(600000), "35-44", "UK", "basic", true)
    )
    
    // Write input tables to local storage
    WriteUtils.writeDatasetToTable(createTestDataset(userEvents), "user_events")
    WriteUtils.writeDatasetToTable(createTestDataset(purchases), "purchases") 
    WriteUtils.writeDatasetToTable(createTestDataset(userProfiles), "lookup_user")
    
    // Execute the job
    UserActivity.run(20240101, 20240101, "user_events", "purchases", "output_table")
    
    // Read and verify output
    val result = FetchUtils.readTableAsDataset[UserActivitySummary]("output_table")
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
