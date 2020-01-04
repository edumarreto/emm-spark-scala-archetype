name := "emm-spark-scala-archetype"

version := "1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core"  % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" % "spark-streaming_2.11" % sparkVersion,
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % sparkVersion,
  "org.apache.spark" % "spark-streaming-kinesis-asl_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.zalando" %% "spark-json-schema" % "0.6.1",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7",
  "org.elasticsearch" % "elasticsearch-hadoop" % "6.4.1",
  "net.jpountz.lz4" % "lz4" % "1.3.0",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.scalamock" %% "scalamock" % "4.1.0" % "test",
  "com.databricks" %% "spark-redshift" % "2.0.1",
  "org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.3.0",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.3.1",
  "com.stratio.datasource" %% "spark-mongodb" % "0.12.0"

)
