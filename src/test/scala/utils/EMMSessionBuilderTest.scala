package utils

import utils.test.UnitSpec


class EMMSessionBuilderTest extends UnitSpec{

  "The FAIO session builder" should "create a Spark Session" in{
    val args = Map("env" -> "local", "AWSACCESSKEY"->"test", "AWSSECRETKEY" -> "test")
    val config = Map("appName" -> "test_local")
    val session = new EMMSessionBuilder(args, config).build()
    val sessionEnv = session.conf.get("spark.master", "local")
    assert(sessionEnv.contains("local"))
  }

}
