package services.databases

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark.sql._
import exceptions.ElastisearchException

class ElasticsearchService(elasticAddress: String) {

  def readElasticsearchIndex(index: String, query:String=""): DataFrame={
    if(index.isEmpty) throw new ElastisearchException("Elasticsearch index not set correctly")
    val spark = SparkSession.builder().getOrCreate()
    try{spark.esDF(index + "/" + query)}
    catch{ case ioe: Exception => throw new ElastisearchException("Elasticsearch search criteria not correctly set " + index + "/" + query)}
  }

  def writeElasticsearchIndex(index: String, query:String="", df: DataFrame): Unit={
    if(index.isEmpty) throw new ElastisearchException("Elasticsearch index not set correctly")
    try{df.saveToEs(index + {if(query.isEmpty) "" else "/" + query})}
    catch{ case ioe: Exception => throw new ElastisearchException("Elasticsearch write error for index " + index + "/" +  query)}
  }
}
