
import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.{col, from_json}

object Kafka2S3Avro {
  def main(args: Array[String]): Unit = {


    val bootstrapServers = "10.200.245.83:9092,10.200.58.127:9092, 10.200.101.134:9092"
    val topic = "rtm_in_app_bidding_mediation_marketplace"

    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Writing avro to FS from avro topic")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)
      .option("startingOffsets", "latest") // From starting
     // .load("/Users/mohsinbanedar/Desktop/Kafka-Aviv/KafkaTestFinal/src/main/scala/person.avsc")
      .load()


   // val personDF = df.select(from_json(col("value"), schema).as(("person"))
     // .select("person.*")
    // val jsonDF = df.selectExpr("CAST(value AS STRING) AS json_data")
  //  val dataDF = jsonDF.select($"timestam",from_json(col("json_data"), schema).as("data"))

    //val dataDF = df.select(from_json(col("json_data"), schema).as("data"))
   // df.select(from_json(col("value"),schema))


    // personDF.printSchema()
  //  personDF.show()
    /*personDF.writeStream
      .format("avro")
     .outputMode("append")
     // .option("header", true)
      //.option("checkpointLocation")
      .option("path", "s3://mobile-streaming-poc/AvroTopicConsumer-CheckpointDirectory10")
     .option("checkpointLocation","s3://mobile-streaming-poc/ActualAvroFiles10")
    //  .option("checkpointLocation", "/Users/mohsinbanedar/Desktop/streaming-poc-testing/checkpoint-avro_files")
     // .option("path", s"/Users/mohsinbanedar/Desktop/streaming-poc-testing/ActualAvroFiles")
      .start()
      .awaitTermination()*/
  }
}

