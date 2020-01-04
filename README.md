# edumarreto - Railway data analysis pipeline


## **Description**

Spark Scala pipeline with 4 major steps:

 - SampleBatchIngestionConverter: converts .csv files into parquet file, optimizing file storage and serialization performance.
 - FileIngestionMongoJob.scala: ingests railway data with 4 major dimensions (2 wheels and 2 boxes) and splits into 4 MongoDB documents for historical analysis;
- KafkaIngestionMongoJob: ingests railways events on Kafka topic, analyse the incidence of anomalies and persist on MongoDB.
- MongoMetricsJob.scala: read MongoDB data and generates metrics, storing the resultset on MongoDB dedicated document.


## **Pre-requirements**

 - Spark environment;
 - Railway data: sample data can be found on /sampledata folder;
 - MongoDB instance. Sample MongoDB setup based on docker:

     > sudo docker run -d --name icd-mongodb -p 27017:27017 -v /home/edumarreto/mongodata/:/data/db mongo:4.1
    > sudo docker run -d --name icd-mongodb -p 27017:27017 -v /home/edumarreto/mongodata/:/data/db -e MONGO_INITDB_ROOT_USERNAME=username -e MONGO_INITDB_ROOT_PASSWORD=password mongo:4.1

## **Execution**

Execution based on spark-submit command with parameters.
