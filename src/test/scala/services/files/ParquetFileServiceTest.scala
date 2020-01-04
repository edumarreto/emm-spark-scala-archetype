package services.files

import exceptions.IOException
import org.apache.spark.sql.SparkSession
import utils.test.UnitSpec

class ParquetFileServiceTest extends UnitSpec {

  "The ParquetFileService write method" should "return nothing -> Unit (void)" in{
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
    val parquet = new ParquetFileService(testDF).write("src/main/resources/tests/parquet",1,true,true,true,true)
    assert(parquet.getClass.toString.contains("void"))
  }

  it should "throw IOException if file path not reachable" in{
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
    assertThrows[IOException]{
      val parquet = new ParquetFileService(testDF).write("",1,true,true,true,true)
      println("error was thrown")}
  }

  "The ParquetFileService writeStream method" should "return nothing -> Unit (void)" in{
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
    val parquet = new ParquetFileService(testDF).writeStream("src/main/resources/tests/parquet","src/main/resources/tests/parquet/checkpoint",1,true,true,true,true)
    assert(parquet.getClass.toString.contains("void"))
  }

  it should "throw IOException if file path not reachable" in{
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
    assertThrows[IOException]{
      val parquet = new ParquetFileService(testDF).writeStream("","src/main/resources/tests/parquet/checkpoint", 1,true,true,true,true)
      println("error was thrown")}
  }

  it should "throw IOException if file checkpoint path not accessible" in{
    val spark = SparkSession.builder().appName("test").config("spark.master", "local").getOrCreate()
    import spark.implicits._
    val testDF = Seq((1, "bat"), (2, "mouse"), (3, "horse")).toDF()
    assertThrows[IOException]{
      val parquet = new ParquetFileService(testDF).writeStream("src/main/resources/tests/parquet","", 1,true,true,true,true)
      println("error was thrown")}
  }

}
