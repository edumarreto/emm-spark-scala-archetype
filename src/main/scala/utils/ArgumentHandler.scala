package utils

class ArgumentHandler(envArgs:Array[String]) {

  def createArgumentsMap(): Map[String, String] ={
    envArgs.map(_.split("=")).map(a=>(a(0), a(1))).toMap
  }

}
