package services.files

import exceptions.IOException
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

class CSVService() extends AbstractFileService {
  override def read(fileName: String): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    try{
    spark.read.format("csv")
      .option("header","true")
      .option("delimiter",",")
      .option("nullValue","")
      .option("treatEmptyValuesAsNulls","true")
      .load(fileName)
    }
    catch{ case ioe: Exception => throw new IOException("Could not reach CSV file: " + fileName)}
  }

  def readWithSchema(fileName: String, schema:StructType): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    try {
      spark.read.format("csv")
        .schema(schema)
        .option("header", "true")
        .option("delimiter", ",")
        .option("nullValue", "")
        .option("treatEmptyValuesAsNulls", "true")
        .load(fileName)
    }
    catch{ case ioe: Exception => throw new IOException("Could not read CSV file: " + fileName + "with specified schema")}
  }
}
