import sbt.Keys._

name := "tweets-kafka-stream"

version := "0.1"

scalaVersion := "2.12.4"



val kafka_streams_scala_version = "0.2.1"
val confluentVersion = "4.1.1"

resolvers ++= Seq (
  Opts.resolver.mavenLocalFile,
  "Confluent" at "http://packages.confluent.io/maven"
)




libraryDependencies in ThisBuild ++= Seq(
  "com.github.scopt" %% "scopt" % "3.5.0",
  "com.iheart" %% "ficus" % "1.4.3")


libraryDependencies ++= Seq("com.lightbend" %%
  "kafka-streams-scala" % kafka_streams_scala_version)


// ujson
libraryDependencies += "com.lihaoyi" %% "ujson" % "0.6.6"

libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"

libraryDependencies += "io.confluent" % "kafka-avro-serializer" % confluentVersion

libraryDependencies += "io.confluent" % "kafka-streams-avro-serde" % "3.3.0"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.1.0"