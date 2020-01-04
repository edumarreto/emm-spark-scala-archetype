package utils

import utils.test.UnitSpec

class ArgumentHandlerTest extends UnitSpec {

  "The argument handler" should "return a Map of values string string" in {
    val args: Array[String] = Array("env=local")
    val argumentMap = new ArgumentHandler(args).createArgumentsMap()
    assert(argumentMap.get("env") == Some("local"))
  }

}
