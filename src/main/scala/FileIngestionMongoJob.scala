import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.bson.Document
import services.databases.MongoService
import services.files.CSVService
import utils.{EMMSessionBuilder, SchemaHelper}

//Spark Object that reads railways monitoring data from CSV file and writes into MongoDB instance
object FileIngestionMongoJob extends App {

  val spark = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/GAS.hbw_historico")
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/GAS.hbw_historico")
    .config("spark.io.compression.codec", "snappy")
    .getOrCreate()

  lazy val envArgs = new utils.ArgumentHandler(args).createArgumentsMap()

  lazy val configFile: Map[String, String] = new utils.ConfigFileHelper(envArgs.get("config").getOrElse(throw new IllegalArgumentException("No configuraton file set on 'config=' argument"))).getProperties()

  //val spark = new FAIOSessionBuilder(envArgs, configFile).build()
  import spark.implicits._

  val fileSchema= new SchemaHelper().getSchema(configFile.get("schemaFileCSV").getOrElse(" "))
  val dataDF = new CSVService().readWithSchema(configFile.get("sourceFile").getOrElse(" "),fileSchema )
  val mongo = new MongoService("localhost","27017","GAS","HBW_historico")
  mongo.write("HBW_historico", dataDF)
}
