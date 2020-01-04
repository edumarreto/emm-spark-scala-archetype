package services.databases
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.mongodb.spark.sql._


class MongoService(url: String, port:String, database: String, document: String, user: String="", password: String="") {
  val spark = SparkSession.builder().getOrCreate()
  //"spark.mongodb.input.uri", "mongodb://localhost:27017/GAS.test"
  //spark.conf.set("spark.mongodb.input.uri", "mongodb://" + this.url + ":" + this.port + "/" + this.database + "." + this.document)
  //spark.conf.set("spark.mongodb.output.uri", "mongodb://"+ this.url + ":" + this.port + "/" + this.database + "." + this.document)

  def read(collection: String = this.document, filterName:String, filterValue:String): DataFrame={
    val readConfig = ReadConfig(Map("collection" -> collection),
      Some(ReadConfig(this.spark)))
    MongoSpark.load(this.spark, readConfig)
  }
/*
  def write(df: DataFrame): Unit={
    MongoSpark.save(df.write.format("json"))
  }
  */
  def write(collection: String, df: DataFrame): Unit={
    val writeConfig = WriteConfig(Map("collection" -> collection, "writeConcern.w" -> "majority"),
      Some(WriteConfig(this.spark)))
    MongoSpark.save(df.write.mode("append").format("json"), writeConfig)
  }

}
