import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json,to_json,struct}
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.streaming.Trigger

//This code will read the json data from kafka topic and write to kafka topic in avro format
//here only checkpoint directory will get created
object Json2Avro2Kafka {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
     // .master("local[1]")
      .appName("Write Json, convert to avro kafka topic")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val schema = new StructType()
      .add("eventId", IntegerType)
      .add("eventTimestamp", StringType)
      .add("categories", StringType)
      .add("clientParams_connectionType", StringType)
      .add("demandType", StringType)
      .add("buyerType", StringType)
      .add("activityType", StringType)

    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.200.245.83:9092,10.200.58.127:9092, 10.200.101.134:9092")
      .option("subscribe", "ironsolver_2")
      .option("startingOffsets", "earliest") // From starting
      .load()
      //  .repartition(args.lift(3).fold(50)(_.toInt))
      .select(from_json($"value".cast("string"), schema).alias("data")
      )
      .select("data.*")


      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "10.200.245.83:9092,10.200.58.127:9092, 10.200.101.134:9092")
      .option("topic", "avro_topic_monday1")
      .option("checkpointLocation","s3://mobile-streaming-poc/Json2Avro_topic_checkpoint_dir/")
     // .option("checkpointLocation", "/Users/mohsinbanedar/Desktop/streaming-poc-testing")
      .option("failOnDataLoss", "false")
      .start()
      .awaitTermination()
  }
}
