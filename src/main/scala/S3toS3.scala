import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.types.DoubleType
//import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
//import org.apache.spark.sql.catalyst.dsl.expressions.DslAttr
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{from_unixtime,date_format}


object S3toS3 {

  def main(args : Array[String]) : Unit = {

    val sparkSession: SparkSession= SparkSession
      .builder()
     //  .master("local[1]")
      .appName("Streaming S3 Avro RTM files to Iceberg ").getOrCreate
    sparkSession.sparkContext.setLogLevel("ERROR")


    val schema = new StructType()
      .add("abActive", StringType)
      .add("abt", StringType)
      .add("adUnit", IntegerType)
      .add("applicationId", IntegerType)
      .add("auctionEndTimestamp", IntegerType) //convert to string
      .add("auctionId", StringType)
      .add("auctionStartTimestamp", IntegerType)   //convert to string
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
      .add("clientParams_clientTimestamp", IntegerType)  //convert to string
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


    val streamingData = sparkSession.readStream
      .option("maxFilesPerTrigger", args.lift(2).fold(600)(_.toInt))
      .format("avro")
      .schema(schema)
      .option("partitionColumns", "event_date,event_hour")
      // .load(s"/Users/mohsinbanedar/Desktop/streaming-iceberg/xxxxxabcdfiles/part-00000-03927289-8e1b-461e-beff-1038a057d70e-c000.avro")
     .load(s"s3://mobile-streaming-poc/20th_December2022/RTM-AVRO-FILES-20December2022")


  val convertedData = streamingData.withColumn("event_date", date_format(from_unixtime(col("eventTimestamp") / 1000), "yyyy-MM-dd"))
    .withColumn("event_hour", hour(from_unixtime(col("eventTimestamp") / 1000)))

    //convertedData.printSchema()
    //  convertedData.show()


    convertedData.writeStream
      .format("iceberg")  //iceberg-->parquet  hive ....iceberg
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .partitionBy("event_date", "event_hour")
     // .option("partition_spec", "event_date,event_hour")
      //.option("path", "dev.iceberg_poc.rtm_iceberg_parquet")
      .option("path","dev.iceberg_poc.rtm_iceberg_table_prod")
      .option("checkpointLocation", "s3://mobile-streaming-poc/20th_December2022/rtm_iceberg_parquet_20December2022_1")
      .start()
      .awaitTermination()


  }

}
