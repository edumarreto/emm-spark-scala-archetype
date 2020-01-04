package utils

import org.apache.spark.sql.SparkSession

class EMMSessionBuilder(arguments: Map[String, String], configFile: Map[String, String]) {

  def build(): SparkSession= {
    SparkSession
      .builder()
      .appName(configFile.get("appName").getOrElse("SparkConverter"))
      .config("spark.master", "local")
      .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .getOrCreate()
  }
}
