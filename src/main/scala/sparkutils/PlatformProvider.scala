package sparkutils

/**
 * Production-grade platform provider that automatically selects the appropriate
 * SparkPlatform implementation based on the runtime environment.
 * 
 * This object serves as a smart factory that:
 * - Automatically detects test vs production environments
 * - Uses reflection to safely load test-specific implementations when available
 * - Falls back gracefully to production implementations
 * - Provides a consistent API regardless of the underlying implementation
 * 
 * The automatic detection ensures that:
 * - Production code remains clean with no test dependencies
 * - Test environments get thread-safe, parallel-execution-capable platforms
 * - No manual configuration is required in application code
 * 
 * Usage:
 * {{{
 *   import sparkutils.PlatformProvider
 *   
 *   val platform = PlatformProvider.platform
 *   val spark = platform.spark
 * }}}
 */
object PlatformProvider {
  
  /**
   * Gets the appropriate SparkPlatform implementation for the current environment.
   * 
   * The selection logic:
   * 1. If running in a test context (ScalaTest or sbt testing), attempts to load TestSparkPlatform
   * 2. If TestSparkPlatform is not available on classpath, falls back to production platform
   * 3. If not in test context, uses production platform
   * 
   * This design ensures that production deployments never have test dependencies,
   * while development and testing environments get enhanced capabilities.
   * 
   * @return SparkPlatformTrait implementation appropriate for the current environment
   */
  def platform: SparkPlatformTrait = {
    if (isTestContext) {
      loadTestPlatform.getOrElse(SparkPlatform)
    } else {
      SparkPlatform
    }
  }
  
  /**
   * Attempts to load the test platform implementation using reflection.
   * 
   * This approach allows the production code to remain completely clean of test dependencies
   * while still providing enhanced test capabilities when the test classpath is available.
   * 
   * @return Some(TestSparkPlatform) if successfully loaded, None otherwise
   */
  private def loadTestPlatform: Option[SparkPlatformTrait] = {
    try {
      // Try to load the test platform from the updated package structure
      val clazz = Class.forName("sparkutils.TestSparkPlatform$")
      val module = clazz.getField("MODULE$").get(null).asInstanceOf[SparkPlatformTrait]
      Some(module)
    } catch {
      case _: ClassNotFoundException =>
        // TestSparkPlatform not available on classpath (normal in production)
        None
      case _: NoSuchFieldException =>
        // MODULE$ field not found (shouldn't happen with Scala objects)
        None
      case _: IllegalAccessException =>
        // Cannot access the field (shouldn't happen with public objects)
        None
      case _: ClassCastException =>
        // Object doesn't implement SparkPlatformTrait (development error)
        None
      case _: Exception =>
        // Any other exception during loading
        None
    }
  }
  
  /**
   * Determines if the current execution context is a test environment.
   * 
   * Detection mechanisms:
   * 1. Thread name contains "ScalaTest" (ScalaTest execution)
   * 2. System property "sbt.testing" is set to "true" (sbt test execution)
   * 
   * @return true if running in a test context, false otherwise
   */
  private def isTestContext: Boolean = {
    val threadName = Thread.currentThread().getName
    val isScalaTest = threadName.contains("ScalaTest")
    val isSbtTesting = System.getProperty("sbt.testing", "false") == "true"
    
    isScalaTest || isSbtTesting
  }
  
  /**
   * Gets information about the current platform selection for debugging purposes.
   * 
   * @return Map containing platform selection details
   */
  def getPlatformInfo: Map[String, String] = {
    val isTest = isTestContext
    val platformType = if (isTest) {
      loadTestPlatform match {
        case Some(_) => "TestSparkPlatform"
        case None => "SparkPlatform (test platform not available)"
      }
    } else {
      "SparkPlatform (production)"
    }
    
    Map(
      "isTestContext" -> isTest.toString,
      "threadName" -> Thread.currentThread().getName,
      "sbtTesting" -> System.getProperty("sbt.testing", "false"),
      "selectedPlatform" -> platformType
    )
  }
}
