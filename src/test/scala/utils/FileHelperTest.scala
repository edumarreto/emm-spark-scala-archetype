package utils

import utils.test.UnitSpec
import exceptions.IOException

class FileHelperTest extends UnitSpec{

  "The File Helper" should "return a Sequence(collection) for getLocalData method" in{
    val fileHelp = new FileHelper("src/main/resources/tests/config-test.properties").getLocalData()
    assert(fileHelp.getClass.toString().contains("collection"))
  }

  it should "return a string for getFileContentAsString method" in{
    val fileHelp = new FileHelper("src/main/resources/tests/config-test.properties").getFileContentAsString()
    assert(fileHelp.getClass.toString.contains("java.lang.String"))
  }

  it should "throw IOException if file path does not exist for method getLocalData" in {

    assertThrows[IOException]{
      val fileHelp = new FileHelper("wrong_test/config-test.properties").getFileContentAsString()
      println("error was thrown")}
  }

  it should "throw IOException if file path does not exist for method getFileContentAsString" in {

    assertThrows[IOException]{
      val fileHelp = new FileHelper("wrong_test/config-test.properties").getFileContentAsString()
      println("error was thrown")}
  }

}
