/*import org.apache.spark.sql.functions.col
import org.apache.spark.sql
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.ZoneOffset
import java.sql.Timestamp
*/
import java.util.{Collections, Properties}
import java.util.Properties
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.sql.Timestamp
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import scala.collection.JavaConverters._

object Kafka2S3Replay {
  def main(args: Array[String]): Unit = {


    val dateTime = LocalDateTime.of(2023, 1, 1, 0, 0, 0) // some date and time
    val zoneId = ZoneId.of("UTC") // the time zone
    val zonedDateTime = dateTime.atZone(zoneId) // the date and time in the specified time zone
    val timestamp = Timestamp.valueOf(zonedDateTime.toLocalDateTime) // the timestamp value
    val topic = "rtm_in_app_bidding_mediation_marketplace"
    val props = new Properties


   // Properties props = new Properties();
    props.put("bootstrap.servers", "10.200.245.83:9092,10.200.58.127:9092,10.200.101.134:9092");
    props.put("group.id", "my-consumer-group");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList(topic))
   // consumer.subscribe(Collections.singletonList(topic))
    consumer.seek(new TopicPartition(topic, 0), timestamp.getTime) // set the consumer position to the timestamp

    import org.apache.kafka.common.TopicPartition;

//
  //  org.apache.kafka.common.TopicPartition tp = new org.apache.kafka.common.TopicPartition("rtm_in_app_bidding_mediation_marketplace", 0);
    //consumer.assign(Collections.singletonList(tp));


    while (true) {
      val records = consumer.poll(Duration.ofMillis(100)) // retrieve the messages from the topic
      for (record <- records.asScala) {
        println(s"Received message with timestamp ${record.timestamp()}: ${record.value()}")
      }
    }
  }
}

  /*  import org.apache.spark.sql.SparkSession

    val spark = SparkSession
      .builder()
      .master("local[1]")
      .getOrCreate()

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


   // val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

   // println("Enter a starting timestamp  in the format yyyy-MM-dd HH:mm:ss:")
    //val dateTimeString = scala.io.StdIn.readLine()
    //val dateTime = LocalDateTime.parse(dateTimeString, dateTimeFormat)

   // val startTimestamp = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli
   // println(s"The timestamp for $dateTime is: $startTimestamp")

   /* val dateTimeFormat1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

    println("Enter a ending timestamp  in the format yyyy-MM-dd HH:mm:ss:")
    val dateTimeString1 = scala.io.StdIn.readLine()
    val dateTime1 = LocalDateTime.parse(dateTimeString1, dateTimeFormat1)

    val endTimestamp = dateTime.toInstant(ZoneOffset.UTC).toEpochMilli
    println(s"The timestamp for $dateTime1 is: $endTimestamp")*/

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.200.245.83:9092,10.200.58.127:9092,10.200.101.134:9092")
      .option("subscribe", "rtm_in_app_bidding_mediation_marketplace")
      .option("startingOffsets", "earliest") // set startingOffsets to "earliest" to start reading from the earliest available offset
      .option("failOnDataLoss", "false")
      .option("maxOffsetsPerTrigger", "1000")
      .option("timestamp", "timestamp") // specify the column that contains the timestamp for each message
      .load()

    val startTimestamp: Long = 1672665056L
    df.printSchema()
    // Filter the dataframe to include only records with a timestamp greater than or equal to the startingTimestamp
    //val filteredDf = df.filter(col("timestamp").geq(startTimestamp))
    val filteredDf = df.filter(col("timestamp").geq(new Timestamp(1672666625)))

    filteredDf.select(from_json(col("value").cast("string"), schema).alias("data")
      ).select("data.*")
    // && col("timestamp").leq(endTimestamp))

    // Start streaming the data
    val query = filteredDf
      .writeStream
      .format("avro")
      .outputMode("append")
      .option("checkpointLocation", "/Users/mohsinbanedar/Desktop/streaming-iceberg/aqwer")
      .option("path", s"/Users/mohsinbanedar/Desktop/streaming-iceberg/sccccabcdfiles")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()

  }
}

   */
/*
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.sql.Timestamp

val dateTime = LocalDateTime.of(2022, 1, 1, 0, 0, 0) // some date and time
val zoneId = ZoneId.of("UTC") // the time zone
val zonedDateTime = dateTime.atZone(zoneId) // the date and time in the specified time zone
val timestamp = Timestamp.valueOf(zonedDateTime.toLocalDateTime) // the timestamp value

val topic = "my-topic"

val consumer = new KafkaConsumer[String, String](properties)
consumer.subscribe(Collections.singletonList(topic))
consumer.seek(new TopicPartition(topic, 0), timestamp.getTime) // set the consumer position to the timestamp

while (true) {
  val records = consumer.poll(Duration.ofMillis(100)) // retrieve the messages from the topic
  for (record <- records.asScala) {
    println(s"Received message with timestamp ${record.timestamp()}: ${record.value()}")
  }
}

*/

/*import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.kafka.common.serialization.StringDeserializer
import java.text.SimpleDateFormat

object Kafka2S3Replay {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[1]")
      .appName("KafkaToS3Streaming")
      .getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka-to-s3-group",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("rtm_in_app_bidding_mediation_marketplace")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // get user input for specific date and time
    val dateTimeInput = sc.getConf.get("spark.dateTimeInput")
    val dateTimeFormat = sc.getConf.get("spark.dateTimeFormat")

    // filter stream based on user input
    val filteredStream = stream.filter(record => {
      val recordTimestamp = new SimpleDateFormat(dateTimeFormat).parse(record.timestamp().toString)
      val inputTimestamp = new SimpleDateFormat(dateTimeFormat).parse(dateTimeInput)
      recordTimestamp.after(inputTimestamp)
    })


    filteredStream.foreachRDD { rdd =>
     // rdd.saveAsTextFile(s"s3a://mobile-streaming-poc/all-fields/")
      rdd.saveAsTextFile(s"/Users/mohsinbanedar/Desktop/streaming-iceberg/sccccabcdfiles")
     // s3://mobile-streaming-poc/all-fields/

    }
    // write stream to S3

  /*  filteredStream.foreachRDD(rdd => {
      val df = spark.createDataFrame(rdd.map(record => (record.key, record.value)), StructType(Array(StructField("key", StringType), StructField("value", StringType))))
      df.write.parquet("s3://bucket/path/to/folder")

    })*/

    ssc.start()
    ssc.awaitTermination()
  }
}

*/