package services.files

import org.apache.spark.sql.DataFrame

abstract class AbstractFileService {

  def read(fileName:String):DataFrame


}
