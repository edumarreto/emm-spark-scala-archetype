package utils

import exceptions.IOException

import scala.io.Source
import scala.io.Source.fromFile

class FileHelper(filePath: String="") {

  val configFile: Map[String, String] = new utils.ConfigFileHelper(filePath).getProperties()



  def getLocalData():Seq[String]={
    try{fromFile(this.filePath).getLines.toSeq}
    catch{ case ioe: Exception => throw new IOException("File set as param for FileHelper class was not found")}

  }


  def getFileContentAsString(): String={
    try{Source.fromFile(this.filePath).getLines.mkString}
    catch{ case ioe: Exception => throw new IOException("File set as param for FileHelper class was not found")}
  }

}
