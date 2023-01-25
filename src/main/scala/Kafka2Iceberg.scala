import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Kafka2Iceberg
{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Kafka to Iceberg")
     // .master("local[1]")
     .getOrCreate()


    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val df = spark.readStream
      .format("kafka")
    //.option("kafka.bootstrap.servers", "127.0.0.1:9092")
    // .option("subscribe", "iceberg")
     .option("kafka.bootstrap.servers", "10.200.245.83:9092,10.200.58.127:9092, 10.200.101.134:9092")
    .option("subscribe", "iceberg_test_1")  //topic
      .option("startingOffsets", "latest")
      .option("failOnDataLoss", "false")
      .load()


    //val resDF = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
   //   .as[(String, String)].toDF("id", "data")


    val schema = new StructType()
      .add("current_day", StringType)
      .add("user_id", StringType)
      .add("page_id", StringType)
      .add("channel", StringType)
      .add("action", StringType)
    //  .add("page_id", StringType)

      val transDF = df.select(from_json($"value".cast("string"), schema).alias("data1")).select("data1.*")
    //val transDF = resDF.withColumn("current_day", split(col("data"), "\t")(0))
      //.withColumn("ts", split(col("data"), "\t")(1))
     // .withColumn("user_id", split(col("data"), "\t")(2))
     // .withColumn("page_id", split(col("data"), "\t")(3))
     // .withColumn("channel", split(col("data"), "\t")(4))
    //  .withColumn("action", split(col("data"), "\t")(5))


//Working code
   // val query: StreamingQuery = transDF.writeStream
    // .outputMode("append")
     // .format("console")
      // .start()
     // query.awaitTermination()


       val query: StreamingQuery = transDF.writeStream
      .format("iceberg")
      .outputMode("append")
       .option("path","dev.iceberg_poc.streaming_avro_test")
      .option("checkpointLocation", "s3://mobile-streaming-poc/Iceberg-checkpoint2")
      .start()
    query.awaitTermination()

    }
  }

