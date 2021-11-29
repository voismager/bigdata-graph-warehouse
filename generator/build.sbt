name := "generator"

version := "0.1"

scalaVersion := "2.12.14"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.1",
  "org.apache.kafka" % "kafka-clients" % "2.8.0",
  "org.apache.kafka" %% "kafka" % "2.8.0",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.12.3"
)
