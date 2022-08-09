package svend.playground.waterflow.http

import com.typesafe.scalalogging.Logger
import org.json4s.*
import org.scalatra.*
import org.scalatra.json.*

class TaskRunnerServlet extends ScalatraServlet with JacksonJsonSupport {

  val logger = Logger("TaskHttpServer")

  given jsonFormats: Formats = org.json4s.DefaultFormats

  post("/runtask") {

    val taskRunRequest = parsedBody.extract[RunTaskRequest]
    logger.info(s"running task with label ${taskRunRequest.taskLabel}")

    // sets the output content-type to JSON. This is typically done in a before(), for all routes 
    contentType = formats("json")

    // TODO: specific returned value for each kind of task
    Ok(RunTaskResponse("all good"))
  }

}

case class RunTaskRequest(taskLabel: String)
case class RunTaskResponse(result: String)