import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProviderChain, DefaultAWSCredentialsProviderChain}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
//import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import  org.apache.hadoop.fs.{s3a,s3native}
import com.amazonaws._


object KafkaProduceAvro {
  def main(args: Array[String]): Unit = {
//THis file will generate data in JSON, convert the data in AVRO(with schema) and load it into
    // another kafka topic called avro_topic1.


 val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()


    val spark: SparkSession = SparkSession.builder()
     .master("local[1]")
      .appName("Read_JSON_convert_to_Avro_and_put_it_in_kafka_topic_avro_topic1")
    // .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .getOrCreate()
  //  spark.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", "FwoGZXIvYXdzEGUaDE/lNUq6r5+GSeL3sCLmAezrd97vqlxAieSLIo/nA7CO9cNTsRciRKOThsSwouarleuBEhqcfmf94v+muxeEm5+SRYluMus0er7Oxu2Y6btlBJCCNhjARTEj8xrNvBHBd2N1tKeeggSVczrXBL5Ns0C0XmRmEYWR5zSW69hLkUJ9R/e1SdmK+ZubCPBIcukpyzqh2LThY+8Zeq9xafaNgKSc2uQ/oqn0poJfSbtje0vcjhsdRSCMiXGheM2kwUxutyqsdO+pMENasw8U9PmdiGdKaife2KYzTtWLGmuhVNI3297YPGSU5mhKf20Hjot+WMgiOgC5KLn+4psGMjLwGBh4eGxOhosmZu6/hXHNEFdAeuwq28wah9ZfK/f+MiuaBTE8j4ODqeG/jzOaUFL/MA==")

     //   spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AWS_ACCESS_KEY_ID")
     // spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret_key", "AWS_SECRET_ACCESS_KEY")
    //  spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint","s3.amazonaws.com")

   // spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")

    /*
    Disable logging as it writes too much log
     */
    spark.sparkContext.setLogLevel("ERROR")
    /*
    This consumes JSON data from Kafkqa
     */
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.200.245.83:9092,10.200.58.127:9092, 10.200.101.134:9092")
      .option("subscribe", "ironsolver_json")
      .option("includeHeaders", "true")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", args.lift(1).fold(500000)(_.toInt))
      .load()

    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    /*
     Prints Kafka schema with columns (topic, offset, partition e.t.c)
      */
    df.printSchema()
    val schema = new StructType()
      .add("firstname",StringType)
      .add("lastname",StringType)
    /*
    Converts JSON string to DataFrame
     */
    val personDF = df.selectExpr("CAST(value AS STRING)") // First convert binary to string
      .select(from_json(col("value"), schema).as("data"))
    personDF.printSchema()

    /*
      * Convert DataFrame columns to Avro format and name it as "value"
      * And send this Avro data to Kafka topic
      */

    personDF.select(org.apache.spark.sql.avro.functions.to_avro(struct("data.*")) as "value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("kafka.bootstrap.servers", "10.200.245.83:9092,10.200.58.127:9092, 10.200.101.134:9092")
      .option("topic", "avro_topic1")
      .option("checkpointLocation", "s3a://mobile-streaming-poc//")
      .start()
      .awaitTermination()
  }
}
