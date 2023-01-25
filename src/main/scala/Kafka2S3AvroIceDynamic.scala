import org.apache.spark.sql.SparkSession
import scala.io.Source

import org.apache.spark.sql.Row

//This code will read the json data from kafka topic and write to kafka topic in avro format
//here only checkpoint directory will get created
object Kafka2S3AvroIceDynamic {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()
    // Read the file from S3

    var configMap=Source.fromFile("/Users/mohsinbanedar/Desktop/streaming-iceberg/app.conf").getLines().filter(line => line.contains("=")).map{ line => val tkns=line.split("=")
      if(tkns.size==1){
        (tkns(0) -> "" )
      }else{
        (tkns(0) ->  tkns(1))
      }
    }.toMap

    val a= configMap("spark.executor.memory")
println(a)


    //val topic: String = topicRow.first()
    //println(topic)
    //val topicValue: String = topic.split("=")(1).trim
   // println(topicValue)
   // df.filter($"value" === "topic").first()



    // Read the configuration file from S3
   // val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
   // val inputPath = new Path("/Users/mohsinbanedar/Desktop/streaming-iceberg/config.txt")
    //val inputStream = fs.open(inputPath)
   // val config = scala.io.Source.fromInputStream(inputStream).getLines.mkString
   // inputStream.close()

   // spark.conf.getAll(config)

    import org.apache.spark.sql.Row


    // Filter the DataFrame to get the row with the max_iterations option
   // val maxIterationsRow: Row = df.filter($"value" === "max_iterations").first()

    // Get the value of the first column in the row
    //val maxIterations: String = maxIterationsRow.getString(0)

    // Split the row on the equals sign to get the option value
    //val maxIterationsValue: String = maxIterations.split("=")(1).trim

    // Use the option value
   // println(maxIterationsValue)


    // val json_read = spark.read.json("/Users/mohsinbanedar/Desktop/Kafka-Aviv/KafkaTestFinal/src/main/scala/spark-config.json")
  //json_read.printSchema()
   // json_read.show()
    //val appName = json_read.select("appName").first().getString(0)
    //println(appName)
  //  val master = .select("spark.master").first().getString(0)
   // spark.conf.set("spark.master", master)
   // println(master)
     // .master("local[1]")
     // .appName("Streaming All RTM Kafka fields to S3 in Avro format")
    //  .getOrCreate()
      /*spark.conf.set("spark.streaming.kafka.consumer.enable.auto.commit", "false")
      spark.conf.set("spark.streaming.kafka.consumer.enable.idempotence", "true")
      spark.conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
      spark.conf.set("spark.streaming.kafka.maxRatePerPartition","12000")*/

    //  val json_read = spark.read.format("json")
       //  .load("/Users/mohsinbanedar/Desktop/Kafka-Aviv/KafkaTestFinal/src/main/scala/spark-config.json")
/*
    val appName = json_read.select("spark.app.name").first().getString(0)
    val master = json_read.select("spark.master").first().getString(0)
    val executorMemory = json_read.select("spark.executor.memory").first().getString(0)
    val driverMemory = json_read.select("spark.driver.memory").first().getString(0)
    val executorCores = json_read.select("spark.executor.cores").first().getInt(0)
    val driverCores = json_read.select("spark.driver.cores").first().getInt(0)*/


   /* spark.conf.set("spark.app.name", appName)
    spark.conf.set("spark.master", master)
    spark.conf.set("spark.executor.memory", executorMemory)
    spark.conf.set("spark.driver.memory", driverMemory)
    spark.conf.set("spark.executor.cores", executorCores)
    spark.conf.set("spark.driver.cores", driverCores)*/




    /*val enableAutoCommit = json_read.select("spark.streaming.kafka.consumer.enable.auto.commit").first()
    val enableIdempotence = json_read.select("spark.conf.spark.streaming.kafka.consumer.enable.idempotence").first().getString(0)
    val writeAheadLogEnabled = json_read.select("spark.conf.spark.streaming.receiver.writeAheadLog.enable").first().getString(0)
    val maxRatePerPartition = json_read.select("spark.conf.spark.streaming.kafka.maxRatePerPartition").first().getString(0)
   // spark.conf.set("spark.streaming.kafka.consumer.enable.auto.commit", enableAutoCommit)
    //spark.conf.set("spark.streaming.kafka.consumer.enable.idempotence", enableIdempotence)
   // spark.conf.set("spark.streaming.receiver.writeAheadLog.enable", writeAheadLogEnabled)
   // spark.conf.set("spark.streaming.kafka.maxRatePerPartition", maxRatePerPartition)
    val autoCommitValue1 = enableAutoCommit.getString(0)
    println(autoCommitValue1);*/
/*
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val bootstrapServers = "10.200.245.83:9092,10.200.58.127:9092,10.200.101.134:9092"
    val topic = "rtm_in_app_bidding_mediation_marketplace"
   val schema = new StructType()
     .add("abActive", StringType)
     .add("abt", StringType)
     .add("adUnit", IntegerType)
     .add("applicationId", IntegerType)
     .add("auctionEndTimestamp", IntegerType)
     .add("auctionId", StringType)
     .add("auctionStartTimestamp", IntegerType)
     .add("auctionType", StringType)
     .add("bidPriceRange_lowerBound", DoubleType)
     .add("bidPriceRange_upperBound", DoubleType)
     .add("bidRequestTimestamp", StringType)
     .add("bidResponseTimestamp", StringType)
     .add("bidType", StringType)
     .add("blockedRequest", StringType)
     .add("clearance", DoubleType)
     .add("clientParams_advId", StringType)
     .add("clientParams_advIdType", StringType)
     .add("clientParams_appVersion", StringType)
     .add("clientParams_applicationUserId", StringType)
     .add("clientParams_auid", StringType)
     .add("clientParams_browserUserAgent", StringType)
     .add("clientParams_bundleId", StringType)
     .add("clientParams_clientTimestamp", IntegerType)
     .add("clientParams_connectionType", StringType)
     .add("clientParams_country", StringType)
     .add("clientParams_deviceHeight", IntegerType)
     .add("clientParams_deviceLang", StringType)
     .add("clientParams_deviceMake", StringType)
     .add("clientParams_deviceModel", StringType)
     .add("clientParams_deviceOS", StringType)
     .add("clientParams_deviceOSVersion", StringType)
     .add("clientParams_deviceType", StringType)
     .add("clientParams_deviceWidth", IntegerType)
     .add("clientParams_fs", IntegerType)
     .add("clientParams_isLimitAdTrackingEnabled", StringType)
     .add("clientParams_mobileCarrier", StringType)
     .add("clientParams_secure", IntegerType)
     .add("companyKey", StringType)
     .add("dynamicDemandSourceType", IntegerType)
     .add("eventId", IntegerType)
     .add("eventTimestamp", StringType)
     .add("instanceId", IntegerType)
     .add("instanceLevelConfiguration", StringType)
     .add("instanceType", IntegerType)
     .add("ip", StringType)
     .add("isAuctionWinner", StringType)
     .add("isCoppa", StringType)
     .add("isMarketplace", StringType)
     .add("isMarketplaceDatasource", StringType)
     .add("isTest", StringType)
     .add("numberOfBidRequests", IntegerType)
     .add("numberOfBidResponses", IntegerType)
     .add("price", DoubleType)
     .add("provider", StringType)
     .add("providerId", IntegerType)
     .add("publisherId", IntegerType)
     .add("rank", IntegerType)
     .add("sdkVersion", StringType)
     .add("sessionDepth", IntegerType)
     .add("sessionId", StringType)
     .add("state", StringType)
     .add("time", StringType)
     .add("userAgent", StringType)
     .add("clientParams_consent", StringType)
     .add("isGdpr", StringType)
     .add("activityType", StringType)
     .add("advertiserDomain", StringType)
     .add("brandsDspName", StringType)
     .add("brandsGrossBid", DoubleType)
     .add("brandsLatency", IntegerType)
     .add("brandsSellerVersion", StringType)
     .add("brandsTemplateType", StringType)
     .add("buyerType", StringType)
     .add("categories", StringType)
     .add("connection", StringType)
     .add("demandBundle", StringType)
     .add("demandCategory", StringType)
     .add("demandType", StringType)

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe",topic)
      .option("startingOffsets","latest")
      .option("failOnDataLoss","false")
      .load()
      val df1 = df.select(from_json($"value".cast("string"), schema).alias("data")
      ).select("data.*")

    val convertedData = df1.withColumn("event_date", date_format(from_unixtime(col("eventTimestamp") / 1000), "yyyy-MM-dd"))
      .withColumn("event_hour", hour(from_unixtime(col("eventTimestamp") / 1000)))
val a = "path"
      convertedData.writeStream
          .format("avro")  //spark
          .outputMode("append")
          .option("header", true)
        .partitionBy("event_date", "event_hour")
         // .trigger(Trigger.ProcessingTime("60 seconds"))
     // .option("checkpointLocation", "/Users/mohsinbanedar/Desktop/streaming-iceberg/2023-2")
    // .option("path", s"/Users/mohsinbanedar/Desktop/streaming-iceberg/2023-3.1")
        .option("path","s3://mobile-streaming-poc/2nd_Jan_2023/RTM-AVRO-FILES-2Jan2023")
        .option("path",a)
          .option("checkpointLocation","s3://mobile-streaming-poc/2nd_Jan_2023/RTM-AVRO-FILES-CHECKPOINT-2Jan2023")
          .start()
          .awaitTermination()*/
  }
}

