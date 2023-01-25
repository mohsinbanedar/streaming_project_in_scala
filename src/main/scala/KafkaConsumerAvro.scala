import com.amazonaws.auth.DefaultAWSCredentialsProviderChain

import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro._
import org.apache.spark.sql.functions.{col, struct}
import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProviderChain, DefaultAWSCredentialsProviderChain}
import org.apache.hadoop.fs.{s3a, s3native}
import com.amazonaws._

object KafkaConsumerAvro {
  def main(args: Array[String]): Unit = {
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .config("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
      .appName("aa")
      .getOrCreate()
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.session.token", "FwoGZXIvYXdzEGUaDE/lNUq6r5+GSeL3sCLmAezrd97vqlxAieSLIo/nA7CO9cNTsRciRKOThsSwouarleuBEhqcfmf94v+muxeEm5+SRYluMus0er7Oxu2Y6btlBJCCNhjARTEj8xrNvBHBd2N1tKeeggSVczrXBL5Ns0C0XmRmEYWR5zSW69hLkUJ9R/e1SdmK+ZubCPBIcukpyzqh2LThY+8Zeq9xafaNgKSc2uQ/oqn0poJfSbtje0vcjhsdRSCMiXGheM2kwUxutyqsdO+pMENasw8U9PmdiGdKaife2KYzTtWLGmuhVNI3297YPGSU5mhKf20Hjot+WMgiOgC5KLn+4psGMjLwGBh4eGxOhosmZu6/hXHNEFdAeuwq28wah9ZfK/f+MiuaBTE8j4ODqeG/jzOaUFL/MA==")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "AWS_ACCESS_KEY_ID")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret_key", "AWS_SECRET_ACCESS_KEY")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
    //   spark.sparkContext.hadoopConfiguration.set("fs.s3a.","s3.amazonaws.com")

    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.readStream
      .format("kafka")
    //  .option("kafka.bootstrap.servers", "192.168.1.100:9092")
      .option("kafka.bootstrap.servers", "10.200.245.83:9092,10.200.58.127:9092, 10.200.101.134:9092")
      .option("subscribe", "avro_topic1")
      .option("failOnDataLoss", "false")
      .option("startingOffsets", "earlist") // From starting
      .load()

    /*
     Prints Kafka schema with columns (topic, offset, partition e.t.c)
      */
    df.printSchema()
    /*
    Read schema to convert Avro data to DataFrame
     */


    val readschema = new String(
     Files.readAllBytes(Paths.get("/Users/mohsinbanedar/Desktop/Kafka-Aviv/KafkaTestFinal/src/main/scala/person.avsc")))

    val personDF = df.select(org.apache.spark.sql.avro.functions.from_avro(col("value"), readschema).as("person"))
    .select("person.*")


    /*
    Stream data toLocal file  for testing and store it in Avro Format

     */
    personDF.select(org.apache.spark.sql.avro.functions.to_avro(struct("person.*")) as "value")
    personDF.writeStream
      .format("avro")
      .outputMode("append")
      .option("header", true)
      //.trigger(Trigger.ProcessingTime(s"${args.lift(2).getOrElse("10")} seconds"))
      .option("checkpointLocation", "s3a://mobile-streaming-poc/avro_landing_checkpoint_location/")
      .option("path", s"s3a://mobile-streaming-poc/avro_landing_actual_avro_files/")

      .start()
      .awaitTermination()




  }
}