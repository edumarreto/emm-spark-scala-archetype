package services.files

import java.util.Calendar

import exceptions.IOException
import org.apache.spark.sql.{DataFrame, SparkSession}

private object PartitionPath {
  final val YEAR = "/year="
  final val MONTH = "/month="
  final val DAY = "/day="
  final val HOUR = "/hour="
  final val EMPTY = ""


  def mount(yearPartition:Boolean=true, monthPartition:Boolean=true, dayPartition:Boolean=true, hourPartition: Boolean= true): String = {
    // TO DO: Isso torna os testes dificultosos
    val calendar = Calendar.getInstance()
    val year= if(yearPartition) YEAR + calendar.get(Calendar.YEAR) else EMPTY
    val month = if(monthPartition) MONTH + calendar.get(Calendar.MONTH) else EMPTY
    val day = if(dayPartition) DAY + calendar.get(Calendar.DAY_OF_MONTH) else EMPTY
    val hour = if(hourPartition) HOUR + calendar.get(Calendar.HOUR_OF_DAY) else EMPTY
    year + month + day + hour
  }
}

class ParquetFileService(data: DataFrame) {

  def write(targetPath: String, instances: Int=1, yearPartition:Boolean=true, monthPartition:Boolean=true, dayPartition:Boolean=true, hourPartition: Boolean= true): Unit = {
    val path = targetPath + PartitionPath.mount(yearPartition, monthPartition, dayPartition, hourPartition)
    try {
      this.data.coalesce(instances).write.mode("append").parquet(path)
    }
    catch{ case ioe: Exception => throw new IOException("Could not write parquet file in folder " + path)}
  }

  def writeStream(targetPath: String, checkpointLocation: String, instances: Int=1, yearPartition:Boolean=true, monthPartition:Boolean=true, dayPartition:Boolean=true, hourPartition: Boolean= true): Unit = {
    val path = targetPath + PartitionPath.mount(yearPartition, monthPartition, dayPartition, hourPartition)
    try {
      this.data.coalesce(instances).writeStream
        .format("parquet")
        .option("checkpointLocation", checkpointLocation)
        .option("path", path).start()
    }
    catch{ case ioe: Exception => throw new IOException("Could not write parquet file in folder " + path)}
  }
}
