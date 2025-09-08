ThisBuild / organization := "com.example"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"

// Scalafix settings
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

lazy val root = (project in file("."))
  .settings(
    name := "scala-spark-template-213",
    
    // Scala 2.13 settings
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xfatal-warnings",
      "-language:higherKinds",
      "-Wunused:imports,privates,locals,explicits,implicits,params",
      "-Xlint:_",
      "-Ywarn-dead-code",
      "-Ywarn-numeric-widen",
      "-Ywarn-value-discard"
    ),
    
    // Spark dependencies (provided to reduce jar size)
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
      "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",
      
      // Development dependencies (for local running)
      "org.apache.spark" %% "spark-core" % "3.5.1" % Compile,
      "org.apache.spark" %% "spark-sql" % "3.5.1" % Compile,
      
      // CLI argument parsing
      "com.github.scopt" %% "scopt" % "4.1.0",
      
      // Iceberg support (production)
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.13" % "1.4.2" % "provided",
      
      // Test dependencies (include Spark for testing)
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.apache.spark" %% "spark-core" % "3.5.1" % Test,
      "org.apache.spark" %% "spark-sql" % "3.5.1" % Test,
      "com.holdenkarau" %% "spark-testing-base" % "3.5.0_1.4.7" % Test
    ),
    
    // Assembly settings for fat jar creation
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) => MergeStrategy.discard
      case x => MergeStrategy.first
    },
    
    // Assembly jar name
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
    
    // Exclude provided dependencies from assembly
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter { jar =>
        jar.data.getName.contains("spark-")
      }
    },
    
    // Test settings with Java 11+ compatibility
    Test / parallelExecution := false,
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "-Xmx2g",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.util=ALL-UNNAMED"
    ),
    
    // Runtime JVM options
    run / javaOptions ++= Seq(
      "-Xmx2g",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.io=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
      "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
      "--add-opens=java.base/javax.security.auth=ALL-UNNAMED",
      "--add-opens=java.base/sun.security.util=ALL-UNNAMED"
    ),
    
    run / fork := true
  )
