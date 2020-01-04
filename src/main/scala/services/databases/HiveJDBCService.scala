package services.databases

import exceptions.HiveJDBCException
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class HiveJDBCService(url: String, port:String, user: String, password: String) {
  def read(table: String): DataFrame={
    val spark = SparkSession.builder().getOrCreate()
    try{
      spark.read
        .format("jdbc")
        .option("url", "jdbc:hive2://"+ this.url + ":" + this.port)
        .option("dbtable", table)
        .option("user", this.user)
        .option("password", this.password)
        .option("fetchsize", "20")
        .option("numPartitions", 10)
        .load()
    }
    catch{ case ioe: Exception => throw new HiveJDBCException("Could not reach Hive server from " + this.url + " on port: " + this.port)}
  }

  def write(df: DataFrame, table: String): Unit={
    val spark = SparkSession.builder().getOrCreate()
    try{
        println("write")

    }
    catch{ case ioe: Exception => throw new HiveJDBCException("Could not reach Hive server from " + this.url + " on port: " + this.port)}
  }

}
