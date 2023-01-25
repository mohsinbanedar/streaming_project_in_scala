import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json,to_json,struct}
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.streaming.Trigger
object Avro2Parquet {

  def main(args : Array[String]) : Unit = {

    val sparkSession: SparkSession = SparkSession.builder.appName("Avro2Parquet").getOrCreate
    sparkSession.sparkContext.setLogLevel("ERROR")
    sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    //val targetPath = args.lift(1).getOrElse(s"${args(0)}/parquet")
    val schema = new StructType()
      .add("firstName", StringType)
      .add("lastName", StringType)

    sparkSession
      .readStream
      .option("maxFilesPerTrigger", args.lift(2).fold(600)(_.toInt))
      .format("avro")
      .schema(schema)
      .load(s"s3://mobile-streaming-poc/ActualAvroFiles")
     //.transform(Schema.input2Output)
      .writeStream
      .format("parquet")
      .option("path","s3://mobile-streaming-poc/ActualParquetFiles")
      .option("checkpointLocation","s3://mobile-streaming-poc/ParquetFiles-checkpoint")
     // .option("path", )
      //.option("checkpointLocation", s"$targetPath/checkpoint/")
     // .partitionBy("event_day", "hour")
      .start
      .awaitTermination
  }

}