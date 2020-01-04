import org.apache.spark.sql.SparkSession
import services.files.{CSVService, ParquetFileService}
import utils.{EMMSessionBuilder, SchemaHelper}

//Sample Scala Object that demonstrates a file format transformation - csv to parquet
object SampleBatchIngestionConverter extends App {
  lazy val envArgs = new utils.ArgumentHandler(args).createArgumentsMap()

  lazy val configFile: Map[String, String] = new utils.ConfigFileHelper(envArgs.get("config").getOrElse(throw new IllegalArgumentException("No configuraton file set on 'config=' argument"))).getProperties()

  val spark = new EMMSessionBuilder(envArgs, configFile).build()
  import spark.implicits._

  val fileSchema= new SchemaHelper().getSchema(configFile.get("schemaFileCSV").getOrElse(" "))
  val dataDF = new CSVService().readWithSchema(configFile.get("sourceFile").getOrElse(" "),fileSchema )
  dataDF.show(10)
  val parquetFile = new ParquetFileService(dataDF)
  parquetFile.write(configFile.get("targetPath").getOrElse("/"))
}