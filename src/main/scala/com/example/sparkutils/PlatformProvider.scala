package com.example.sparkutils

object PlatformProvider {
  def platform: SparkPlatformTrait =
    if (isTestContext) {
      // Use reflection to get TestSparkPlatform from test classpath
      try {
        val clazz  = Class.forName("com.example.sparkutils.TestSparkPlatform$")
        val module = clazz.getField("MODULE$").get(null).asInstanceOf[SparkPlatformTrait]
        module
      } catch {
        case _: ClassNotFoundException =>
          // TestSparkPlatform not available, fall back to production platform
          SparkPlatform
        case _: Exception =>
          SparkPlatform
      }
    } else {
      SparkPlatform
    }

  private def isTestContext: Boolean =
    Thread.currentThread().getName.contains("ScalaTest") ||
      System.getProperty("sbt.testing", "false") == "true"
}
