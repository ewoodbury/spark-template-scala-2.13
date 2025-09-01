# Scala 2.13 Spark Template

A production-ready template project for Apache Spark applications using Scala 2.13.

## Features

- **Scala 2.13 + Spark 3.5**: Latest stable versions for Spark in production
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

- Java 17 (recommend [jEnv](https://www.jenv.be/) for managing Java versions)
- SBT 1.x

### Available Commands

The project includes a Makefile with convenient commands:

```bash
make help       # Display all available commands
make test       # Run tests
make lint       # Run linting and formatting
make test-lint  # Run linting checks without fixing
```

### Running the Application

1. Compile with `sbt compile`
2. Run tests with `make test`
3. Run the sample application with `sbt "runMain com.example.SparkApp"`

### Code Quality

- Format and lint code: `make lint`
- Check formatting and linting: `make test-lint`

### Building for Deployment

Create a fat JAR for cluster deployment:
```bash
sbt assembly
```

The assembled JAR will be created in `target/scala-2.13/` and can be submitted to a Spark cluster using `spark-submit`.

## Why Scala 2.13

This template uses Scala 2.13, which offers several production advantages over Scala 3:

- **Native Spark compatibility** - No cross-version complications
- **Mature ecosystem** - Full library compatibility
- **Scala 3 Compatibility** - Interoperable with Scala 3 syntax, enabling upgrade when Scala 3 is supported by Spark

### Java Version Notes

I highly recommend Java 17 for all new Spark projects, because:
- **Java 8/11**: Spark supports these versions, but they lack garbage collection optimizations and other performance improvements found in Java 17.
- **Java 18+**: Not compatible with Spark

The project includes necessary JVM flags for Java 17, which are automatically applied when using `sbt run` or `make test`.

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
make test
```

## CI in GitHub Actions

The template includes Github Actions workflows for regular Scala tests, linting, and formatting checks. The CI runs on every PR and commit on the main branch.

## Production Deployment

1. Build the assembly JAR: `sbt assembly`
2. Submit to a Spark cluster:
```bash
spark-submit \
  --class com.example.SparkApp \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.13/scala-spark-template-213-0.1.0-SNAPSHOT.jar
```
