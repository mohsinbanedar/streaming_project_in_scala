import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
//import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
//import org.apache.spark.sql.catalyst.dsl.expressions.DslAttr
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}


object S3toS3_1 {

  def main(args : Array[String]) : Unit = {

    val sparkSession: SparkSession= SparkSession
      .builder()
   // .master("local[1]")
      .appName("Streaming S3 Avro files to Iceberg ").getOrCreate
    sparkSession.sparkContext.setLogLevel("ERROR")


    /*val schema = new StructType()
      .add("eventId", IntegerType)
      .add("eventTimestamp", StringType)
      .add("categories", StringType)
      .add("clientParams_connectionType", StringType)
      .add("demandType", StringType)
      .add("buyerType", StringType)
      .add("activityType", StringType)
*/

    val streamingData = sparkSession.readStream
      .option("maxFilesPerTrigger", args.lift(2).fold(600)(_.toInt))
      .format("avro")
      .schema(Schema.dataStructType)
     .load(s"s3://mobile-streaming-poc/RTM-ACTUAL-AVRO-FILES-all")


    val convertedData = streamingData.withColumn("event_date", date_format(from_unixtime(col("eventTimestamp") / 1000), "yyyy-MM-dd"))
      .withColumn("event_hour", hour(from_unixtime(col("eventTimestamp") / 1000)))




    convertedData.printSchema()
   // convertedData.show()
  /*  val convertedData = streamingData
      .withColumn("event_date", to_timestamp(date_format(col("eventTimestamp"), "yyyy-MM-dd")))
      .withColumn("event_hour", to_timestamp(date_format(col("eventTimestamp"), "HH")))*/

    /*val convertedData = streamingData.select(
      col("eventTimestamp"),
      to_timestamp(date_format(col("eventTimestamp"), "yyyy-MM-dd")) as "event_date",
      to_timestamp(date_format(col("eventTimestamp"), "HH")) as "event_hour"
    )*/
    // Write the streaming data to the table, partitioning it by the date and hour columns


    convertedData.writeStream
      .format("iceberg")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("60 seconds"))
      .partitionBy("event_date", "event_hour")
     // .option("partition_spec", "event_date,event_hour")
      .option("path", "dev.iceberg_poc.rtm_iceberg_table_parquet10")
      .option("checkpointLocation", "s3://mobile-streaming-poc/IcebergS3RTM-10")
      .start()
      .awaitTermination()


   /* val query = convertedData.writeStream
      .outputMode("append")
      .format("console")
      .start()
    query.awaitTermination()*/


  }

}

