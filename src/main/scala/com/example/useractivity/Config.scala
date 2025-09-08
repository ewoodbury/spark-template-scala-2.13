package com.example.useractivity

case class Config(
  startDate: Int = 0,
  endDate: Int = 0,
  userEventsTable: String = "",
  purchasesTable: String = "",
  outputTable: String = ""
)
