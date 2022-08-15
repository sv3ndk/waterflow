package svend.playground.waterflow.http

import com.typesafe.scalalogging.Logger
import org.json4s
import org.json4s.jackson.JsonMethods.{parse as json4sparse, render as json4srender}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write as json4swrite
import org.json4s.{ShortTypeHints, jvalue2extractable}
import requests.{RequestFailedException, Response, headers}
import svend.playground.waterflow.*

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

/**
 * Task dispatcher requesting the task execution to a remote HTTP server
 * using Haoyi's synchronous Request library
 * */
class RemoteTaskRunner(taskServerUrl: String) extends TaskRunner {

  val logger = Logger(classOf[RemoteTaskRunner])

  given json4s.Formats =
    Serialization.formats(ShortTypeHints(List(classOf[LocalTask], classOf[SparkTask], classOf[SshTask]))) +
      json4s.ext.JInstantSerializer

  override def run(task: Task)(using ec: ExecutionContext): Future[RunLog] = {
    submitTask(task)
      .map {
        response => {
          logger.debug(s"Received Ok HTTP response: ${response.text()}")
          val parsed = json4sparse(response.text()).extract[RunTaskResponse]
          if (parsed.isSuccess)
            parsed.runLog
          else
            throw RetryableTaskFailure(s"task execution status is failed: $parsed")
        }
      }
      .recoverWith {
        case rfe: RequestFailedException if (rfe.response.statusCode == 503) =>
          Future.failed(RetryableTaskFailure(rfe.response.text()))
      }
  }

    /**
     * This does a blocking POST call within a Future => this block a thread :(
     * the API I used is not asynchronous though.
     *
     * The only reason this is a separate method is to make the class more testable,
     * I'm not sure there's "less OOP" way of structuring code in a testable way?
     */
    protected def submitTask(task: Task)(using ExecutionContext): Future[Response] = {
      Future {
        val jsonTask = json4swrite(RunTaskRequest(task))
        logger.debug(s"posting this to remote task runner: $jsonTask")
        // post will throw an exception in case status code is an error
        requests.post(taskServerUrl, data = jsonTask, headers = Map("Content-Type" -> "application/json"))
      }
    }

}
