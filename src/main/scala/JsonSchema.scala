import org.apache.spark.sql.SparkSession

object JsonSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark-JSON-Schema")
      .getOrCreate()

    // Read JSON data
    val df = spark.read.json("/Users/mohsinbanedar/Desktop/streaming-iceberg/JSON")

    // Convert JSON data to JSON schema
    val schema = df.schema.json

    // Print JSON schema
    println(schema)
  }
}