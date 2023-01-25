import org.apache.arrow.flatbuf.TimeUnit
import org.apache.iceberg.expressions.True
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, LongType}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

object KafkaIcebergRaw {


  def main(args: Array[String]): Unit = {

    // Configure the parameters for the catalog.
    /*val sparkConf = new SparkConf()
    sparkConf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    sparkConf.set("spark.sql.catalog.dlf_catalog", "org.apache.iceberg.spark.SparkCatalog")
    sparkConf.set("spark.sql.catalog.dlf_catalog.catalog-impl", "org.apache.iceberg.aliyun.dlf.DlfCatalog")
    sparkConf.set("spark.sql.catalog.dlf_catalog.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    sparkConf.set("spark.sql.catalog.dlf_catalog.oss.endpoint", "<yourOSSEndpoint>")
    sparkConf.set("spark.sql.catalog.dlf_catalog.warehouse", "<yourOSSWarehousePath>")
    sparkConf.set("spark.sql.catalog.dlf_catalog.access.key.id", "<yourAccessKeyId>")
    sparkConf.set("spark.sql.catalog.dlf_catalog.access.key.secret", "<yourAccessKeySecret>")
    sparkConf.set("spark.sql.catalog.dlf_catalog.dlf.catalog-id", "<yourCatalogId>")
    sparkConf.set("spark.sql.catalog.dlf_catalog.dlf.endpoint", "<yourDLFEndpoint>")
    sparkConf.set("spark.sql.catalog.dlf_catalog.dlf.region-id", "<yourDLFRegionId>")*/


    val spark = SparkSession
      .builder()
     // .config(sparkConf)
      .appName("StructuredSinkIceberg-RTM-8 fields")
      .master("local[1]")
      .getOrCreate()

    val checkpointPath = "s3://mobile-streaming-poc/Iceberg-checkpoint-Raw-RTM"
    val bootstrapServers = "10.200.245.83:9092,10.200.58.127:9092, 10.200.101.134:9092"
    val topic = "rtm_in_app_bidding_mediation_marketplace"

    // Read data from the Kafka cluster.
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
     .option("startingOffsets", "latest")
      //.option("failOnDataLoss", "false")
      .load()
//df.printSchema()
    import spark.implicits._
   //val resDF = df.selectExpr( "CAST(value AS STRING)")  //kafka json data


      val schema = new StructType()
         .add("eventId", IntegerType)
         .add("eventTimestamp",LongType)
         .add("categories", StringType)
       .add("clientParams_connectionType",StringType)
         .add("demandType",StringType)
        .add("buyerType", StringType)
       .add("activityType",StringType)


   val df1 = df.select(from_json($"value".cast("string"),schema).alias("data")).select("data.*")
     // .withColumn("eventTimestamp1",to_date(from_unixtime(df("eventTimestamp"))))

    /* val columnNameToCheck="eventTimestamp"
    if(df1.columns.contains(columnNameToCheck))
      println("column exists")
    else
      println("column not exists")*/

    val  df2 = df1.withColumn("eventTimestamp",to_date(from_unixtime(df1("eventTimestamp"))))

    df2.printSchema()


    val query = df1.writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()

   // resDF.printSchema()
    // Write data to the Iceberg table in streaming mode.
/*
   val query = df1.writeStream
      .format("iceberg")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("path", "dev.iceberg_poc.rtm_iceberg_table")
      .option("checkpointLocation", checkpointPath)
      .start()

    query.awaitTermination()*/
  }
}
