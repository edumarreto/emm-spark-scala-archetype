package services.streams

import org.apache.spark.sql.SparkSession._
import utils.test.UnitSpec
import exceptions.StreamException

class KafkaServiceTest extends UnitSpec{

  "The KafkaService readTopic method" should "shoud return a Dataset even if no schema is set" in{
    val spark = builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
    val kafkaMock = mock[KafkaService]
    (kafkaMock.readTopic _).expects(*) returning(testDF)
    val mockDF = kafkaMock.readTopic()
    assert(mockDF.getClass.toString.contains("org.apache.spark.sql.Dataset"))
  }

  it should "throw an error if kafka URL and topic are wrong" in{
    val spark = builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    assertThrows[StreamException]{
      val kafka = new KafkaService("wrongURL","wrongtopic").readTopic()
      println("error was thrown")}
  }

    "The KafkaService readTopic method" should "shoud return a Dataset if a schema is defined" in{
      val spark = builder().appName("test").config("spark.master", "local").getOrCreate()
      import spark.implicits._
      val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
      val kafkaMock = mock[KafkaService]
      (kafkaMock.readTopic _).expects(*) returning(testDF)
      val mockDF = kafkaMock.readTopic("src/main/resources/tests/person.json")
      assert(mockDF.getClass.toString.contains("org.apache.spark.sql.Dataset"))
    }

}
