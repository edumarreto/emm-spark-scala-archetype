package services.databases


import exceptions.ElastisearchException
import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import utils.test.UnitSpec


class ElasticsearchServiceTest extends UnitSpec with MockFactory{

  "The Elasticsearch read method" should "return a Dataset" in{
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
    val esMock = mock[ElasticsearchService]
    (esMock.readElasticsearchIndex _).expects(*, *) returning(testDF)
    val mockDF = esMock.readElasticsearchIndex("test")
    assert(mockDF.getClass.toString.contains("org.apache.spark.sql.Dataset"))
  }

  it should "throw an error if ES index is not set" in{
    assertThrows[ElastisearchException]{
      val es = new ElasticsearchService("localhosttest").readElasticsearchIndex("")
      println("error was thrown")}
  }

  "The Elasticsearch write method" should "send message and return Unit (void)" in{
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
    val esMock = mock[ElasticsearchService]
    (esMock.writeElasticsearchIndex _).expects(*, *, *)
    val result = esMock.writeElasticsearchIndex("test", "test", testDF)
    assert(result.getClass.toString.contains("void"))
  }

  it should "throw an error if ES index is not set" in{
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
    assertThrows[ElastisearchException]{
      val es = new ElasticsearchService("localhosttest").writeElasticsearchIndex("","",testDF)
      println("error was thrown")}
  }

}
