package com.example.useractivity

import scopt.OParser
import sparktoolbox.PlatformProvider

object App {
  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("user-activity-job"),
        head("User Activity ETL Job", "1.0"),
        opt[Int]("start-date")
          .required()
          .action((x, c) => c.copy(startDate = x))
          .text("Start date in YYYYMMDD format"),
        opt[Int]("end-date")
          .required()
          .action((x, c) => c.copy(endDate = x))
          .text("End date in YYYYMMDD format"),
        opt[String]("user-events-table")
          .required()
          .action((x, c) => c.copy(userEventsTable = x))
          .text("User events source table name"),
        opt[String]("purchases-table")
          .required()
          .action((x, c) => c.copy(purchasesTable = x))
          .text("Purchase transactions source table name"),
        opt[String]("output-table")
          .required()
          .action((x, c) => c.copy(outputTable = x))
          .text("Output table name"),
        help("help").text("prints this usage text"),
      )
    }

    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        try {
          // Initialize production Spark session
          PlatformProvider.platform.spark

          // Production-specific operations can go here
          // (e.g., setting up Iceberg catalogs, registering UDFs, etc.)

          // Run the main ETL pipeline
          UserActivity.run(
            config.startDate,
            config.endDate,
            config.userEventsTable,
            config.purchasesTable,
            config.outputTable,
          )

        } finally
          PlatformProvider.platform.stop()
      case _ =>
        // arguments are bad, error message will have been displayed
        System.exit(1)
    }
  }
}
