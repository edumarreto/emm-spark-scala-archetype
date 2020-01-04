package utils

import exceptions.{IOException, SchemaException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.types._
import org.zalando.spark.jsonschema.SchemaConverter

import scala.io.Source.fromFile


class SchemaHelper() {


  def getSchema(filepath:String):StructType={
    filepath.slice(filepath.indexOf(".")+1, filepath.length) match {
      case "csv" => getSchemaFromCSV(filepath)
      case "txt" => getSchemaFromCSV(filepath)
      case "json" => getSchemaFromJSON(filepath)
      case default => throw new SchemaException("No method implemented for such schema in parameter")
    }
  }

  def getSchemaFromCSV(filepath:String):StructType={
    try {
      val lines = fromFile(filepath).getLines.toSeq
      val filteredLines = lines.map(_.trim).filter(!_.startsWith("#")).filter(_.contains(";"))
      val schemaList: List[StructField] = filteredLines.map(
        line => {
          val Array(a, b, c) = line.split(";", 3)
          try{
          StructField(
            a.trim,
            CatalystSqlParser.parseDataType(b.trim),
            c.trim.toBoolean)
          }
          catch{ case ioe: Exception => throw new SchemaException("Could not schema file format")}
        })
        .toList
      StructType(schemaList)
    }
    catch{ case ioe: Exception => throw new IOException("File set as param for getSchemaFromCSV method was not found")}
  }

  def getSchemaFromJSON(filepath:String):StructType={
    val fileContents = new FileHelper(filepath)
    SchemaConverter.convertContent(fileContents.getFileContentAsString())
  }
}
