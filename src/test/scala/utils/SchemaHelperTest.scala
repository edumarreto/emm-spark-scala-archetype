package utils

import exceptions.IOException
import utils.test.UnitSpec

class SchemaHelperTest extends UnitSpec{

  "The SchemaHelper.getSchemaJson method" should "return a StructType for given Json schema file" in{
    val testSchema = new SchemaHelper().getSchemaFromJSON("src/main/resources/tests/person.json")
    assert(testSchema.getClass.toString.contains("org.apache.spark.sql.types.StructType"))
  }

  "The SchemaHelper .getSchemaCSV method" should "return a StructType for a given Json csv file" in{
    val testSchema = new SchemaHelper().getSchemaFromCSV("src/main/resources/tests/person.txt")
    assert(testSchema.getClass.toString.contains("org.apache.spark.sql.types.StructType"))
  }

  it should "throw IOException if text file does not exist" in{

    assertThrows[IOException]{
      val testSchema = new SchemaHelper().getSchemaFromJSON("wrong_path/tests/person.txt")
      println("error was thrown")}
  }
  /*
  it should "return SchemaException in case of unknow schema type" in{
    val testSchema = new SchemaHelper().getSchema("src/main/resources/tests/person.fakeschema")
    assertThrows[SchemaException]{println("error was thrown")}
  }
*/
}
