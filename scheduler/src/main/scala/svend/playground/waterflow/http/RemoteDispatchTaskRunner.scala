package svend.playground.waterflow.http

import com.typesafe.scalalogging.Logger
import dispatch.*
import org.asynchttpclient.Response
import org.json4s
import org.json4s.JsonAST.JValue
import org.json4s.jackson.JsonMethods.{parse as json4sparse, render as json4srender}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write as json4swrite
import org.json4s.{ShortTypeHints, jvalue2extractable}
import svend.playground.waterflow.*

import java.nio.charset.Charset
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
 * Task dispatcher requesting the task execution to a remote HTTP server
 * using Dispatch library, which is essentially a scala DSL around AsyncHttpClient.
 * I think I don't like that DSL that much because:
 *  - it makes the code a bit obscure, with methods like <:< or > to set headers or execute the request
 *  - it's not fully documented
 *  - the json parsing with jackson is not configurable (can't pass a custom format)
 *  - the useful part, i.e. transforming a Java ListenableFuture to Scala Future, is easily replicated
 * */
class RemoteDispatchTaskRunner(taskServerUrl: String) extends TaskRunner {

  val logger = Logger(classOf[RemoteDispatchTaskRunner])

  given json4s.Formats =
    Serialization.formats(ShortTypeHints(List(classOf[LocalTask], classOf[SparkTask], classOf[SshTask]))) +
      json4s.ext.JInstantSerializer

  override def run(task: Task)(using ec: ExecutionContext): Future[RunLog] = {
    submitTask(task)
      .flatMap {
        response => {
          if (response.getStatusCode == 200) {
            logger.debug(s"Received Ok HTTP response: $response")
            // BUG here: I'm not handling JSON parsing errors... ^^
            val parsed = json4sparse(response.getResponseBody()).extract[RunTaskResponse]
            if (parsed.isSuccess)
              Future.successful(parsed.runLog)
            else
              Future.failed(throw RetryableTaskFailure(s"task execution status is failed: $response"))
          } else if (response.getStatusCode == 503) {
            Future.failed(RetryableTaskFailure(response.getResponseBody()))
          } else {
            Future.failed(new RuntimeException("Unexpected error while submitting task to worker."))
          }
        }
      }
  }

  def submitTask(task: Task)(using ExecutionContext): Future[Response] = {

    def toJson: Future[String] = Future {
      val jsonTask = json4swrite(RunTaskRequest(task))
      logger.debug(s"posting this to remote task runner: $jsonTask")
      jsonTask
    }

    def submit(jsonTask: String): Future[Response] =
      Http.default(
        url(taskServerUrl)
          .POST
          .setContentType("application/json", Charset.forName("UTF-8"))
          .addHeader("Accept", "application/json")
          .setBody(jsonTask)
      )

    toJson.flatMap(submit)

  }

}
