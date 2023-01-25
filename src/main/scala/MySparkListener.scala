
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.{SparkListener,SparkListenerTaskEnd, SparkListenerJobStart}
import java.io.{BufferedReader, InputStreamReader, OutputStreamWriter}
import java.net.URL
import javax.net.ssl.HttpsURLConnection

class MySparkListener extends SparkListener {
   override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    // Store the start time of the job

   lazy val startTime = System.currentTimeMillis()
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    // Check if the job has been running for more than 10 minutes


    lazy val startTime: Long = System.currentTimeMillis()
    val currentTime = System.currentTimeMillis()
    if (currentTime - startTime > 600000) {
      // Send a notification that the job is stuck
      sendNotification(s"Task ${taskEnd.taskInfo.taskId} has ended")
    }
  }

  def sendNotification(message: String): Unit = {
    // Set up the Slack API URL and the payload
    val url = new URL("https://slack.com/api/chat.postMessage")
    val payload = s"""{"channel": "#my-channel", "text": "$message"}"""

    // Set up the connection
    val conn = url.openConnection().asInstanceOf[HttpsURLConnection]
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.setRequestProperty("Content-Type", "application/json")
    conn.setRequestProperty("Authorization", "Bearer xoxp-your-slack-api-token")

    // Send the request
    val writer = new OutputStreamWriter(conn.getOutputStream())
    writer.write(payload)
    writer.flush()

    // Read the response
    val reader = new BufferedReader(new InputStreamReader(conn.getInputStream()))
    val response = reader.readLine()

    // Print the response
    println(response)
  }
}

