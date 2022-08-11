package svend.playground.waterflow.http

import com.typesafe.scalalogging.Logger
import org.json4s
import org.json4s.jackson.JsonMethods.{parse as json4sparse, render as json4srender}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write as json4swrite
import org.json4s.{ShortTypeHints, jvalue2extractable}
import requests.{Response, headers}
import svend.playground.waterflow.{LocalTask, RunLog, SparkTask, SshTask, Task, TaskDispatcher}

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

/**
 * Task dispatcher requesting the task execution to a remote HTTP server. 
 * */
class RemoteDispatcher(taskServerUrl: String) extends TaskDispatcher{

  val logger = Logger(classOf[RemoteDispatcher])

  given json4s.Formats =
    Serialization.formats(ShortTypeHints(List(classOf[LocalTask], classOf[SparkTask], classOf[SshTask]))) +
      json4s.ext.JInstantSerializer

  override def run(task: Task)(using ec: ExecutionContext): Future[RunLog] =

  // This does a blocking POST call within a Future => this block a thread :( 
  // the API I used is not asynchronous though
    Future {

      val postBody = json4swrite(RunTaskRequest(task, 1))
      logger.debug(s"posting this to remote task runner: $postBody")

      requests.post(
        taskServerUrl,
        data = json4swrite(RunTaskRequest(task, 1)),
        headers = Map("Content-Type" -> "application/json")
      )
    }
      // TODO: 2 error handling cases to add here:
      // failed task
      // HTTP error and timeouts
      .map(response => json4sparse(response.text()).extract[RunTaskResponse].runLog)


}
