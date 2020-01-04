package utils

import utils.test.UnitSpec
import exceptions.IOException

class ConfigFileHelperTest extends UnitSpec {

  "The configuration file helper" should "return a Map of values string string" in {
    val configMap = new ConfigFileHelper("src/main/resources/tests/config-test.properties").getProperties()
    assert(configMap.get("test") == Some("tested"))
  }

  it should "throw IOException if the file do not exist" in {

    assertThrows[IOException]{
      val configMap = new ConfigFileHelper("failedlocation/config-test.properties").getProperties()
      println("error was thrown")}
  }


}
