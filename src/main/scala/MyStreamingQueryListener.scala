import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.sql.streaming.StreamingQueryListener._

class MyStreamingQueryListener extends StreamingQueryListener {

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    val progress = event.progress
    progress.numInputRows
    progress.inputRowsPerSecond
    // Use progress information to log metrics or update a UI, for example
    println("Query made progress")

  }

  override def onQueryStarted(event: QueryStartedEvent): Unit = {
    val queryName = event.name
    println(s"Query $queryName started")

  }

 override def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
   val exception = event.exception
   println(s"Query terminated with exception: $exception")
 }
}
