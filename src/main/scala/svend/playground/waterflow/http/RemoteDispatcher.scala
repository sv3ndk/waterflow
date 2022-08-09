package svend.playground.waterflow.http

import requests.{Response, headers}
import svend.playground.waterflow.{Dispatcher, RunLog, Task}

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

/**
 * Task dispatcher requesting the task execution to a remote HTTP server. 
 * */
class RemoteDispatcher(taskServerUrl: String) extends Dispatcher {

  override def run(task: Task)(using ec: ExecutionContext): Future[RunLog] =

  // This does a blocking POST call within a Future => this block a thread :( 
  // the API I used is not asynchronous though
    Future {
      val startTime = Instant.now()
      val response: Response = requests.post(
        taskServerUrl,
        // TODO: use json4s here + pass more task parameters
        data = s""" {"taskLabel" : "${task.label}"} """,
        headers = Map("Content-Type" -> "application/json")
      )
      RunLog(startTime, response.text())
    }

}
