package svend.playground.waterflow.http

import com.typesafe.scalalogging.Logger
import org.json4s
import org.json4s.jackson.Serialization
import org.json4s.{ShortTypeHints, jvalue2extractable}
import org.scalatra.*
import org.scalatra.json.*
import svend.playground.waterflow.{LocalTask, RunLog, SparkTask, SshTask, Task}

import java.time.Instant

class TaskExecutorServlet extends ScalatraServlet with JacksonJsonSupport {

  val logger = Logger(classOf[TaskExecutorServlet])

  given jsonFormats: json4s.Formats =
    Serialization.formats(ShortTypeHints(List(classOf[LocalTask], classOf[SparkTask], classOf[SshTask]))) +
      json4s.ext.JInstantSerializer

  before() {
    // sets the output content-type to JSON, for all routes
    contentType = formats("json")
  }

  post("/runtask") {
    val taskRunRequest = parsedBody.extract[RunTaskRequest]
    logger.info(s"Processing $taskRunRequest")

    val startTime = Instant.now()
    Thread.sleep(10)
    // TODO: specific returned value for each kind of task
    val runLog = RunLog(startTime, "all good")

    Ok(RunTaskResponse(runLog, true, taskRunRequest.attempt))
  }

}

case class RunTaskRequest(task: Task, attempt: Int)
case class RunTaskResponse(runLog: RunLog, isSuccess: Boolean, attempt: Int)