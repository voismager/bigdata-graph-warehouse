name := "spark-streaming"

version := "0.1"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "3.1.2" % "provided",
  "org.apache.spark" %% "spark-core" % "3.1.2",
  "org.apache.spark" %% "spark-sql" % "3.1.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.1.2",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.1.2",
  "org.apache.spark" %% "spark-mllib" % "3.1.2",
  "com.typesafe" % "config" % "1.4.1",
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.apache.kafka" %% "kafka" % "2.8.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.3"
)
