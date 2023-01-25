import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext}

//This code will read the json data from kafka topic and write to kafka topic in avro format
//here only checkpoint directory will get created
object Kafka2S3AvroIce {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
  .master("local[1]")

      .appName("Streaming Kafka to S3 in Avro format")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")
    val bootstrapServers = "10.200.245.83:9092,10.200.58.127:9092, 10.200.101.134:9092"
    val topic = "rtm_in_app_bidding_mediation_marketplace"
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

    // Enable checkpointing and specify the checkpoint directory
    ssc.checkpoint("/path/to/checkpoint/directory")
    val schema = new StructType()
      .add("eventId", IntegerType)
      .add("eventTimestamp", StringType)
      .add("categories", StringType)
      .add("clientParams_connectionType", StringType)
      .add("demandType", StringType)
      .add("buyerType", StringType)
      .add("activityType", StringType)
   // Consuemr

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest") // From starting
      .option("failOnDataLoss", "false")
      .load()
      .select(from_json($"value".cast("string"), schema).alias("data")
      ).select("data.*")


     .writeStream
          .format("avro")
          .outputMode("append")
          .option("header", true)
          //.option("checkpointLocation")
           //.trigger(Trigger.ProcessingTime("60 seconds"))
          .option("path", "s3://mobile-streaming-poc/RTM-ACTUAL-AVRO-FILES-final")
         .option("checkpointLocation", "s3://mobile-streaming-poc/RTM-ACTUAL-AVRO-FILES-CHECKPOINT-final")
        // .option("checkpointLocation", "/Users/mohsinbanedar/Desktop/streaming-iceberg/checkpoint-avro_files4")
       //  .option("path", s"/Users/mohsinbanedar/Desktop/streaming-iceberg/ActualAvroFiles4")
          .start()
          .awaitTermination()
  }
}