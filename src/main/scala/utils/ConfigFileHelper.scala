package utils
import scala.io.Source.fromFile
import exceptions.IOException

class ConfigFileHelper(filename: String) {
/*
  def getProperties(): Map[String,String] = {
    val prop = new Properties()
    prop.load(getClass().getResourceAsStream(filename))
    import scala.collection.JavaConverters._
    prop.entrySet().asScala.foreach {
      (entry) => {
        sys.props += ((entry.getKey.asInstanceOf[String], entry.getValue.asInstanceOf[String]))
      }
    }
    prop.asScala.map(kv => (kv._1,kv._2)).toMap
  }*/
def getProperties(): Map[String,String] = {
  try{
    val lines = fromFile(this.filename).getLines.toSeq
    val cleanLines = lines.map(_.trim)
      .filter(!_.startsWith("#"))
      .filter(_.contains("="))
    cleanLines.map(line => { val Array(a,b) = line.split("=",2); (a.trim, b.trim)}).toMap
  }
  catch{
    case ioe : Exception => throw new IOException("could not get properties from given constructor parameter")
  }
}
}

