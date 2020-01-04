package services.streams

import java.sql.Timestamp

import exceptions.StreamException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.SchemaHelper


class KafkaService(servers: String, topics: String) {
  def readTopic(schemaFile: String=""): DataFrame={

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    try{
      val rawStreamDF = spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", servers)
        .option("subscribe",topics)
        .option("failOnDataLoss", false)
        .load()

      if(schemaFile.isEmpty)
        rawStreamDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
          .as[(String, String, Timestamp)].toDF()
      else{
        val schema = new SchemaHelper().getSchema(schemaFile)
        rawStreamDF.select($"value" cast "string" as "json")
          .select(from_json($"json", schema) as "data")
          .select("data.*")
      }
    }
    catch{ case ioe: Exception => throw new StreamException("Could not reach Kafka topic " + this.topics)}
  }
}
