# Scala 2.13 Spark Template

A production-ready template project for Apache Spark applications using Scala 2.13.

## Features

- Scala 2.13 + Spark 3.5: Latest stable versions for Spark in production
- **Assembly plugin** - Create fat JAR for deployment
- **Test setup** - ScalaTest configured with Spark testing utilities
- **Formatting and Linting** - Pre-configured Scalafmt and Scalafix
- **Production optimized** - Enhanced compiler flags and runtime configurations

Note: If you want to try out using Scala 3 with Spark, check out the Scala 3 version [here](https://github.com/ewoodbury/spark-template-scala-3).

## Project Structure

```
├── build.sbt                    # Build configuration
├── project/
│   └── plugins.sbt             # SBT plugins
├── src/
│   ├── main/
│   │   ├── scala/              # Main Scala source files
│   │   └── resources/          # Resources (log4j2.xml, etc.)
│   └── test/
│       ├── scala/              # Test source files
│       └── resources/          # Test resources
├── .scalafmt.conf              # Scalafmt configuration
├── .scalafix.conf              # Scalafix configuration
└── .gitignore                  # Git ignore patterns
```

## Getting Started

### Prerequisites

- Java 11+ (recommend [jEnv](https://www.jenv.be/) for managing Java versions)
- SBT 1.x

### Running the Application

1. Compile with `sbt compile`
2. Run tests with `sbt test`
3. Run the sample application with `sbt "runMain com.example.SparkApp"`

### Code Quality

- Format code: `sbt scalafmt`
- Check formatting: `sbt scalafmtCheck`
- Apply automatic fixes: `sbt scalafix`

### Building for Deployment

Create a fat JAR for cluster deployment:
```bash
sbt assembly
```

The assembled JAR will be created in `target/scala-2.13/` and can be submitted to a Spark cluster using `spark-submit`.

## Scala 2.13 Advantages

This template uses Scala 2.13, which offers several production advantages over Scala 3:

- **Native Spark compatibility** - No cross-version complications
- **Mature ecosystem** - Full library compatibility
- **Production stability** - Battle-tested in enterprise environments
- **Better tooling support** - More mature IDE and build tool integration

### Scala 2.13 Specific Features

- **Improved collections library** - Better performance and API
- **Enhanced implicits** - More predictable implicit resolution
- **Better type inference** - Especially with complex Spark operations
- **Literal types** - For compile-time constants

Example of Scala 2.13 DataFrame creation with case classes:
```scala
import spark.implicits._

case class Person(name: String, age: Int)
val data = Seq(Person("Alice", 25), Person("Bob", 30))
val df = data.toDF()

// Type-safe operations
val adults = df.filter($"age" >= 18)
val adultNames = df.as[Person].filter(_.age >= 18).map(_.name)
```

## Java Compatibility

### Recommended Java Versions
- **Java 11**: Recommended for development and production
- **Java 17**: Supported with additional JVM flags
- **Java 21+**: Limited compatibility, may have issues with local testing

### Important Java 24+ Compatibility Note
If you're using Java 24 or newer (like Java 24.0.1), you may encounter compatibility issues with Spark's local mode and testing due to security changes in newer Java versions. This is a known limitation.

**For Java 24+ users:**
- **Production deployment**: Works fine (Spark runs on cluster with appropriate Java version)
- **Local development**: May have issues with `sbt test` and local Spark session creation
- **Recommended solution**: Use Java 11 or Java 17 for local development

**Workaround for Java 24+ local development:**
```bash
# Install Java 11 or 17 (using SDKMAN example)
sdk install java 11.0.20-tem
sdk use java 11.0.20-tem

# Then run sbt commands
sbt compile
sbt test
sbt "runMain com.example.SparkApp"
```

### Java 17+ Configuration
If using Java 17 or newer, the project includes necessary JVM flags for both runtime and testing. These are automatically applied when using `sbt run` or `sbt test`.

## Dependencies

- **Spark Core & SQL**: Marked as `provided` to reduce JAR size for cluster deployment
- **ScalaTest**: For unit testing with Spark testing utilities
- **SBT Assembly**: For creating deployable JARs
- **Scalafmt & Scalafix**: For code formatting and linting

## Configuration

- **Logging**: Configured via `log4j2.xml` to reduce Spark's verbose output during development
- **Spark dependencies**: Scoped as `provided` for production deployments
- **Assembly**: Configured with proper merge strategies for Spark applications
- **Compiler flags**: Enhanced for production-ready code quality

## Example Usage

See `src/main/scala/com/example/SparkApp.scala` for a complete example of:
- Setting up a Spark session
- Creating DataFrames with both explicit schemas and case classes
- Performing transformations and typed operations
- Using Scala 2.13 implicits for cleaner code
- Proper resource cleanup

## Testing

The template includes a comprehensive test setup:
- Spark session management in tests
- DataFrame and Dataset testing examples
- Proper test isolation and cleanup

**Running tests:**
```bash
sbt test
```

**Note for Java 24+ users:** If tests fail with `UnsupportedOperationException: getSubject is not supported`, this is due to Java 24+ compatibility issues with Spark's local mode. Use Java 11 or Java 17 for local testing.

**Alternative testing approach for newer Java versions:**
- Tests can be written and compiled successfully
- Use integration testing on actual Spark clusters
- Deploy to production and run integration tests there

## Production Deployment

1. Build the assembly JAR: `sbt assembly`
2. Submit to Spark cluster:
```bash
spark-submit \
  --class com.example.SparkApp \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.13/scala-spark-template-213-0.1.0-SNAPSHOT.jar
```

## Why Scala 2.13 over Scala 3?

While Scala 3 offers modern language features, Scala 2.13 provides:
- **Zero compatibility issues** with Spark and the broader ecosystem
- **Proven stability** in production environments
- **Immediate access** to all Spark features without workarounds
- **Better tooling** and IDE support
- **Easier team onboarding** with familiar syntax
