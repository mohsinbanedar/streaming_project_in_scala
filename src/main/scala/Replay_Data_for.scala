import scala.io.StdIn.readLine
import org.apache.iceberg.data.orc.GenericOrcWriters.timestamp
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.Ordering.Implicits._

object Replay_Data_for {

  def main(args: Array[String]): Unit = {
    // Create a SparkConf and SparkContext
    val conf = new SparkConf()
      .setAppName("KafkaStreamingExample")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Create a StreamingContext with a batch interval of 10 seconds
    val ssc = new StreamingContext(sc, Seconds(10))

    // Set up the Kafka parameters and create a direct stream
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.200.245.83:9092,10.200.58.127:9092, 10.200.101.134:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-streaming-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("rtm_in_app_bidding_mediation_marketplace")

    // Read the data from Kafka, starting from the earliest available offset
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    val pahela: Int = readLine("enter how many number").toInt




    // Get the input date range from the user
    val fromDate = readLine("Enter start date (YYYY-MM-DD): ")
    val toDate = readLine("Enter end date (YYYY-MM-DD): ")

   /* // Filter the stream to only include records with timestamps within the specified date range
   // val filteredStream = stream.filter(record => {
      val timestamp = record.timestamp()
      //timestamp.ge(fromDate).and(timestamp.le(toDate))

    // timestamp>=fromDate && timestamp <= toDate

    })

    // Perform any necessary transformations on the data and write the results to a sink (e.g. a database or file)
    filteredStream.foreachRDD(rdd => {
      rdd.foreach(record => {

      })
    })*/

    // Start the streaming context and wait for it to be stopped
    ssc.start()
    ssc.awaitTermination()

  }
}


