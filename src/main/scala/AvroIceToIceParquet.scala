import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json,to_json,struct}
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.streaming.Trigger
object AvroIceToIceParquet {

  def main(args : Array[String]) : Unit = {

    val sparkSession: SparkSession= SparkSession
      .builder.appName("Streaming S3 Avro files to Iceberg ").getOrCreate
     sparkSession.sparkContext.setLogLevel("ERROR")
   // sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    //val targetPath = args.lift(1).getOrElse(s"${args(0)}/parquet")

    val schema = new StructType()
      .add("eventId", IntegerType)
      .add("eventTimestamp", StringType)
      .add("categories", StringType)
      .add("clientParams_connectionType", StringType)
      .add("demandType", StringType)
      .add("buyerType", StringType)
      .add("activityType", StringType)

    sparkSession.readStream
      .option("maxFilesPerTrigger", args.lift(2).fold(600)(_.toInt))
      .format("avro")
      .schema(schema)
      .load(s"s3://mobile-streaming-poc/RTM-ACTUAL-AVRO-FILES")
      //.transform(Schema.input2Output)

     // val df1 = df.select(from_json($"value".cast("string"), schema).alias("data")
    //  ).select("data.*")



      .writeStream
      .format("iceberg")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("60 seconds"))

      .option("path", "dev.iceberg_poc.rtm_iceberg_table_parquet1")
      .option("checkpointLocation", "s3://mobile-streaming-poc/IcebergS3RTM-1")
      .start()
    .awaitTermination()

  }

}