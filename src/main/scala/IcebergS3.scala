import org.apache.arrow.flatbuf.TimeUnit
//import org.apache.avro.Schema.Parser
import org.apache.iceberg.shaded.org.apache.avro.Schema
import org.apache.iceberg.avro.AvroSchemaUtil
import org.apache.iceberg.expressions.True
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.avro.to_avro
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.iceberg.{CatalogUtil, PartitionSpec}
import org.apache.iceberg.avro.{AvroSchemaUtil, AvroSchemaVisitor, SchemaToType}
import org.apache.avro.Schema.Parser
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.types.{CheckCompatibility, Types}

object IcebergS3 {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      // .config(sparkConf)
      .appName("Iceberg Avro to Iceberg Parquet")
      //.master("local[1]")
      .getOrCreate()

    val checkpointPath = "s3://mobile-streaming-poc/Iceberg-checkpoint-Iceberg2Iceberg"
   // val bootstrapServers = "10.200.245.83:9092,10.200.58.127:9092, 10.200.101.134:9092"
    //val topic = "rtm_in_app_bidding_mediation_marketplace"

    val df = spark.readStream
      .format("iceberg")
    //.option("stream-from-timestamp", Long.toString(streamStartTimestamp))
      .load("iceberg_poc.rtm_iceberg_table")

   /* val schema = new StructType()
      .add("eventId", IntegerType)
      .add("eventTimestamp", LongType)
      .add("categories", StringType)
      .add("clientParams_connectionType", StringType)
      .add("demandType", StringType)
      .add("buyerType", StringType)
      .add("activityType", StringType)*/
   /*val schemaStr1 ="""
                     |{
                     |  "type": "record",
                     |  "name": "iceberg2iceberg",
                     |  "namespace": "test.db_gb18030_test.tbl_test",
                     |  "fields": [
                     |    {"name": "id", "type": ["null", "int"], "default": null},
                     |    {"name": "name", "type": ["null", "string"], "default": null},
                     |    {"name": "age", "type": ["null", "int"], "default": null},
                     |    {"name": "_op", "type": ["null", "string"], "default": null}
                     |  ]
                     |}
                     |""".stripMargin

    val  schemaStr2 =  new  Parser ().parse(schemaStr1)
    val shadedSchema1 =  new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(schemaStr2.toString())
    val icebergSchema1 = AvroSchemaUtil.toIceberg(shadedSchema1);*/

    val query = df.writeStream
      .format("iceberg")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("path", "dev.iceberg_poc.rtm_iceberg_table_parquet")
      .option("checkpointLocation", checkpointPath)
      .start()

  }

}
