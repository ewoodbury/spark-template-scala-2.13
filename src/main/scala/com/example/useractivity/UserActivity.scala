package com.example.useractivity

import org.apache.spark.sql.SparkSession
import sparktoolbox.{PlatformProvider, Fetchers, Writers}
import com.example.useractivity.model._
import com.example.useractivity.transformations.UserActivityTransformations

object UserActivity {
  def run(
      startDate: Int,
      endDate: Int,
      userEventsTable: String,
      purchasesTable: String,
      outputTable: String,
  ): Unit = {
    implicit val spark: SparkSession = PlatformProvider.platform.spark
    import spark.implicits._

    // Read source tables
    val userEvents = Fetchers.readTableAsDataset[UserEvent](userEventsTable)
    val purchases = Fetchers.readTableAsDataset[PurchaseTransaction](purchasesTable)
    val userProfiles = Fetchers.readTableAsDataset[UserProfile]("lookup_user")

    // Apply transformations
    val filteredEvents =
      UserActivityTransformations.filterAndDedupeUserEvents(userEvents, startDate, endDate)
    val filteredPurchases =
      UserActivityTransformations.filterAndDedupePurchases(purchases, startDate, endDate)

    val enrichedEvents =
      UserActivityTransformations.enrichUserEventsWithProfiles(filteredEvents, userProfiles)
    val enrichedPurchases =
      UserActivityTransformations.enrichPurchasesWithProfiles(filteredPurchases, userProfiles)

    val userActivitySummary =
      UserActivityTransformations.aggregateUserActivity(enrichedEvents, enrichedPurchases)

    // Write output
    Writers.writeDatasetToTable(userActivitySummary, outputTable)
  }
}
