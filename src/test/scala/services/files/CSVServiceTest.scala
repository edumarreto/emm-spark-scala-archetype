package services.files

import exceptions.IOException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import utils.test.UnitSpec

class CSVServiceTest extends UnitSpec {

  "The CSVService read method" should "return a Dataset" in{
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
    val csvMock = mock[CSVService]
    (csvMock.read _).expects(*) returning(testDF)
    val csvDF = csvMock.read("teste.csv")
    assert(csvDF.getClass.toString.contains("org.apache.spark.sql.Dataset"))
  }

  it should "throw IOException if file not reachable" in{
    assertThrows[IOException]{
      val es = new CSVService().read("wrongfilepath/file.csv")
      println("error was thrown")}
  }

  "The CSVservice read with schema method" should "return a Dataset" in{
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
    val schema = StructType(
                      StructField("id", IntegerType, true) ::
                        StructField("animal", StringType, false) ::
                        Nil)
    val csvMock = mock[CSVService]
    (csvMock.readWithSchema _).expects(*,*) returning(testDF)
    val csvDF = csvMock.readWithSchema("teste.csv", schema)
    assert(csvDF.getClass.toString.contains("org.apache.spark.sql.Dataset"))
  }

  it should "throw IOException if file not reachable" in{
    assertThrows[IOException]{
      val schema = StructType(
        StructField("id", IntegerType, true) ::
          StructField("animal", StringType, false) ::
          Nil)
      val es = new CSVService().readWithSchema("wrongfilepath/file.csv", schema)
      println("error was thrown")}
  }

}
