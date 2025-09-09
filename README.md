# Scala 2.13 Spark Template

A production-ready template project for Apache Spark applications using Scala 2.13.

## Features

- **Ready for Development**: Start writing Spark code with no setup required
- **Clean Workflows**: Run common actions with a single `make` command
- **Pre-built Test Setup** - Unit and end-to-end data pipelines tests with Spark testing utilities
- **sbt-assembly Plugin** - Create a fat JAR for deployment with one command
- **Formatting and Linting** - Pre-configured Scalafmt and Scalafix
- **Optimized for Production** - Project is designed for large jobs that operate over billions of records
- **Functional Programming Best Practices** - Idiomatic Scala code with immutability, pure functions, and type safety of all data transformations

Note: If you want to try out using Scala 3 with Spark, check out the Scala 3 version [here](https://github.com/ewoodbury/spark-template-scala-3).

## Project Structure

```text
├── build.sbt                    # Build configuration
├── project/
│   └── plugins.sbt             # SBT plugins
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   ├── com/example/useractivity/
│   │   │   │   ├── App.scala                  # Sample application entrypoint
│   │   │   │   └── UserActivity.scala         # Core ETL job logic
│   │   │   └── sparktoolbox/                  # Core Spark utilities
│   │   │       ├── SparkPlatformTrait.scala   # Trait for platform abstraction
│   │   │       ├── SparkPlatform.scala        # Production Spark session manager
│   │   │       ├── PlatformProvider.scala     # Factory for platform selection
│   │   │       ├── Fetchers.scala             # Type-safe reading utilities
│   │   │       └── Writers.scala              # Production-grade writing utilities
│   │   └── resources/                         # Configurations (log4j2.xml, etc.)
│   └── test/
│       ├── scala/
│       │   ├── com/example/useractivity/
│       │   │   ├── TestE2EUserActivity.scala  # End-to-end pipeline tests
│       │   │   └── UserActivityTransformationsTest.scala # Unit tests for transformations
│       │   └── com/example/test/
│       │       └── SparkTestBase.scala        # Base trait for parallel test execution
│       └── resources/                         # Test resources
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
3. Run the sample application in Spark local mode with `sbt "runMain com.example.useractivity.App"`

### Code Quality

- Format and lint code: `make lint`
- Check formatting and linting: `make test-lint`

### Building for Deployment

Create a fat JAR for cluster deployment using `sbt-assmebly` plugin:
```bash
make build
```

The assembled JAR will be created in `target/scala-2.13/` and can be submitted to a Spark cluster using `spark-submit`.

## Why Scala 2.13?

This template uses Scala 2.13, the latest Scala version officially supported by Spark.

It is possible to use Scala 3 with Spark thanks to Scala 2.13 and Scala 3 cross-compatibility, but you may have library issues. As of mid-2025, I recommend sticking to Scala 2.13 for production applications.

If you want to give Scala 3 a try, check out my Scala 3 version of this same template [here](https://github.com/ewoodbury/spark-template-scala-3).

### Java Version

You should match your Java version to the version that your production cluster Spark cluster is using.

If you have control, I'd highly recommend Java 17 for all new Spark projects, because:
- **Java 8/11**: Supported by Spark, b lack garbage collection optimizations and other performance improvements found in Java 17.
- **Java 18+**: Not compatible with Spark.

The project includes necessary JVM flags for Java 17 by default, which are automatically applied when using `sbt run` or `make test`.

If you need to manage multiple Java versions on your machine, I recommend using [jEnv](https://www.jenv.be/).

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

See `src/main/scala/com/example/useractivity/App.scala` for a complete example of:
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

1. Build the assembly JAR: `make build`
2. Submit to a Spark cluster:
```bash
spark-submit \
  --class com.example.userActivity.App \
  --master yarn \
  --deploy-mode cluster \
  target/scala-2.13/scala-spark-template-213-0.1.0-SNAPSHOT.jar
```


## Key Principles

The template is built with several opinionated principles in mind that encourage safer, more robust data engineering:

1. Testing as a Top-Priority
    - Testing is fully setup from the start. All new logic should be covered as a baseline expectation, just like more traditional software engineering projects.
2. Type Safety
    - The project uses Scala traits and case classes to define the schema not only of each table, but also of each intermediate dataframe. This makes the transformations clear to develoeprs, and it can help catch errors during compilation.
3. Pure Functions and Immutability
    - Every transformation function is pure, with no side effects or dependency on external state. This makes the code easier to reason about and test. Dataframes are never modified in place; instead, new dataframes are returned from each transformation.

I encourage you to follow these principles when building out your own data pipelines, whether or not you're using this template.