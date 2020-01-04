import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.joda.time.format.DateTimeFormat
import com.stratio.datasource.mongodb.config.{MongodbConfig, MongodbCredentials, MongodbSSLOptions}
import services.databases.MongoService
import utils.SchemaHelper


object KafkaIngestionMongoJob extends App {

  val envArgs = new utils.ArgumentHandler(args).createArgumentsMap()
  val configFile: Map[String, String] = new utils.ConfigFileHelper(envArgs.get("config").getOrElse(throw new IllegalArgumentException("No configuraton file set on 'config=' argument"))).getProperties()
  val fileSchema= configFile.get("schemaFileCSV").getOrElse(" ")
  val schema = new SchemaHelper().getSchema(fileSchema)

  val spark = SparkSession.builder().master("local[*]").appName("KafkaMongo").config("spark.mongodb.input.uri", "mongodb://127.0.0.1/GAS.hbw_metrics").config("spark.mongodb.output.uri", "mongodb://127.0.0.1/GAS").getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
  val kafkaParams = Map[String, Object]("bootstrap.servers" -> "localhost:9092","key.deserializer" -> classOf[StringDeserializer], "value.deserializer" -> classOf[StringDeserializer], "group.id" -> "test-consumer-group", "auto.offset.reset" -> "latest", "enable.auto.commit" -> (false: java.lang.Boolean))

  val MongoDbHistoryOptions = Map(MongodbConfig.Host -> "localhost:27017",
    MongodbConfig.Database -> "GAS",
    MongodbConfig.Collection -> "hbw_historico")

  val MongoDbFailureCaixa1Options = Map(MongodbConfig.Host -> "localhost:27017",
    MongodbConfig.Database -> "GAS",
    MongodbConfig.Collection -> "hbw_fault_caixa1")

  val MongoDbFailureCaixa2Options = Map(MongodbConfig.Host -> "localhost:27017",
    MongodbConfig.Database -> "GAS",
    MongodbConfig.Collection -> "hbw_fault_caixa2")

  val MongoDbFailureRoda1Options = Map(MongodbConfig.Host -> "localhost:27017",
    MongodbConfig.Database -> "GAS",
    MongodbConfig.Collection -> "hbw_fault_roda1")

  val MongoDbFailureRoda2Options = Map(MongodbConfig.Host -> "localhost:27017",
    MongodbConfig.Database -> "GAS",
    MongodbConfig.Collection -> "hbw_fault_roda2")

  val mongometricsDF = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("localhost:27017", "mongodb://127.0.0.1/GAS.hbw_metrics").load()
  mongometricsDF.createOrReplaceTempView("metrics")

  val topic1=Array("hbw")

  import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

  val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topic1, kafkaParams))
  val elementDstream = stream.map(v => v.value).foreachRDD { rdd =>
    val df=spark.read.schema(schema).json(rdd)
    if(df.count() > 0)
    {
      println("Event region:" + df.select("rh").head().get(0))
      val metricsDF = spark.sql("SELECT * FROM metrics WHERE regiao="  + df.select("rh").head().get(0) + " ORDER BY processed_time DESC LIMIT 1")
      var failureIndicator = false
      //FALHA NA CAIXA 1
      //Insert into Mongo CAIXA 1 document on case of CAIXA_1 failure
      if(metricsDF.count()>0) {
        if((df.select("temp_caixa_1").head().get(0).asInstanceOf[Integer] > metricsDF.select("amp_sup_caixa_1").head().get(0).asInstanceOf[Double]) ||
          (df.select("temp_caixa_1").head().get(0).asInstanceOf[Integer] < metricsDF.select("amp_inf_caixa_1").head().get(0).asInstanceOf[Double]))
            {
              println("Failure Caixa1: " + metricsDF.select("amp_inf_caixa_1").head().get(0) + "<" + df.select("temp_caixa_1").head().get(0) + " <" + metricsDF.select("amp_sup_caixa_1").head().get(0))
              df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").options(MongoDbFailureCaixa1Options).save()
              failureIndicator = true
            }
        else println("Caixa_1: ok -> " + metricsDF.select("amp_inf_caixa_1").head().get(0) + "<" + df.select("temp_caixa_1").head().get(0) + " <" + metricsDF.select("amp_sup_caixa_1").head().get(0))

        //FALHA NA CAIXA 2
        //Insert into Mongo CAIXA 2 document on case of CAIXA_2 failure
        if((df.select("temp_caixa_2").head().get(0).asInstanceOf[Integer] > metricsDF.select("amp_sup_caixa_2").head().get(0).asInstanceOf[Double]) ||
          (df.select("temp_caixa_2").head().get(0).asInstanceOf[Integer] < metricsDF.select("amp_inf_caixa_2").head().get(0).asInstanceOf[Double]))
        {
          println("Failure Caixa 2: " + metricsDF.select("amp_inf_caixa_2").head().get(0) + "<" + df.select("temp_caixa_2").head().get(0) + " <" + metricsDF.select("amp_sup_caixa_2").head().get(0))
          df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").options(MongoDbFailureCaixa2Options).save()
          failureIndicator = true
        }
        else println("Caixa_2: ok -> " + metricsDF.select("amp_inf_caixa_2").head().get(0) + "<" + df.select("temp_caixa_2").head().get(0) + " <" + metricsDF.select("amp_sup_caixa_2").head().get(0))

        //FALHA NA RODA 1
        //Insert into Mongo RODA 1 document on case of RODA_1 failure
        if((df.select("temp_roda_1").head().get(0).asInstanceOf[Integer] > metricsDF.select("amp_sup_temp_roda_1").head().get(0).asInstanceOf[Double]) ||
          (df.select("temp_roda_1").head().get(0).asInstanceOf[Integer] < metricsDF.select("amp_inf_temp_roda_1").head().get(0).asInstanceOf[Double]))
        {
          println("Failure Roda 1: " + metricsDF.select("amp_inf_temp_roda_1").head().get(0) + "<" + df.select("temp_roda_1").head().get(0) + " <" + metricsDF.select("amp_sup_temp_roda_1").head().get(0))
          df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").options(MongoDbFailureRoda1Options).save()
          failureIndicator = true
        }
        else println("Roda_1: ok -> " + metricsDF.select("amp_inf_temp_roda_1").head().get(0) + "<" + df.select("temp_roda_1").head().get(0) + " <" + metricsDF.select("amp_sup_temp_roda_1").head().get(0))

        //FALHA NA RODA 2
        //Insert into Mongo RODA 2 document on case of RODA_2 failure
        if((df.select("temp_roda_2").head().get(0).asInstanceOf[Integer] > metricsDF.select("amp_sup_temp_roda_2").head().get(0).asInstanceOf[Double]) ||
          (df.select("temp_roda_2").head().get(0).asInstanceOf[Integer] < metricsDF.select("amp_inf_temp_roda_2").head().get(0).asInstanceOf[Double]))
        {
          println("Failure Roda 2: " + metricsDF.select("amp_inf_temp_roda_2").head().get(0) + "<" + df.select("temp_roda_2").head().get(0) + " <" + metricsDF.select("amp_sup_temp_roda_2").head().get(0))
          df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").options(MongoDbFailureRoda2Options).save()
          failureIndicator = true
        }
        else println("Roda_2: ok -> " + metricsDF.select("amp_inf_temp_roda_2").head().get(0) + "<" + df.select("temp_roda_2").head().get(0) + " <" + metricsDF.select("amp_sup_temp_roda_2").head().get(0))

      }
      if (!failureIndicator)
        df.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").options(MongoDbHistoryOptions).save()

    }
  }
  ssc.start()
  ssc.awaitTermination
  println("process is running")
}
