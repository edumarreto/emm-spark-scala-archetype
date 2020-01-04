import java.util.Calendar

import FileIngestionMongoJob.args
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import java.sql.Date
import services.databases.MongoService
import services.files.CSVService
import utils.SchemaHelper
import java.time.Instant

//Scala object that reads data from Mongo Document hbw_historico and extracts metrics based on fixed params on SQL query
object MongoMetricsJob extends App{
  val spark = SparkSession.builder()
    .master("local")
    .appName("MongoSparkConnectorIntro")
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/GAS.hbw_historico")
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/GAS.hbw_historico")
    .config("spark.io.compression.codec", "snappy")
    .getOrCreate()

  lazy val envArgs = new utils.ArgumentHandler(args).createArgumentsMap()

  lazy val configFile: Map[String, String] = new utils.ConfigFileHelper(envArgs.get("config").getOrElse(throw new IllegalArgumentException("No configuraton file set on 'config=' argument"))).getProperties()

  import spark.implicits._

  val mongo = new MongoService("localhost","27017","GAS","HBW_historico")
  val hbw_df = mongo.read("hbw_historico","","")
  hbw_df.show(5)
  hbw_df.createOrReplaceTempView("HBW_DF")

  val metricsDF = spark.sql(
    "SELECT rh as regiao, (AVG(temp_caixa_1) + (1.5 * (PERCENTILE(temp_caixa_1, 0.75) - PERCENTILE(temp_caixa_1, 0.25)))) as amp_sup_caixa_1,(AVG(temp_caixa_1) - (1.5 * (PERCENTILE(temp_caixa_1, 0.75) - PERCENTILE(temp_caixa_1, 0.25)))) as amp_inf_caixa_1,(AVG(temp_caixa_2) + (1.5 * (PERCENTILE(temp_caixa_2, 0.75) - PERCENTILE(temp_caixa_2, 0.25)))) as amp_sup_caixa_2,(AVG(temp_caixa_2) - (1.5 * (PERCENTILE(temp_caixa_2, 0.75) - PERCENTILE(temp_caixa_2, 0.25)))) as amp_inf_caixa_2,(AVG(temp_roda_1) + (1.5 * (PERCENTILE(temp_roda_1, 0.75) - PERCENTILE(temp_roda_1, 0.25)))) as amp_sup_temp_roda_1,(AVG(temp_roda_1) - (1.5 * (PERCENTILE(temp_roda_1, 0.75) - PERCENTILE(temp_roda_1, 0.25)))) as amp_inf_temp_roda_1,(AVG(temp_roda_2) + (1.5 * (PERCENTILE(temp_roda_2, 0.75) - PERCENTILE(temp_roda_2, 0.25)))) as amp_sup_temp_roda_2,(AVG(temp_roda_2) - (1.5 * (PERCENTILE(temp_roda_2, 0.75) - PERCENTILE(temp_roda_2, 0.25)))) as amp_inf_temp_roda_2,current_timestamp() as processed_time FROM HBW_DF GROUP BY rh"
  )
  mongo.write("hbw_metrics", metricsDF)
}
