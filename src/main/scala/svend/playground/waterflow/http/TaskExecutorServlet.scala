package svend.playground.waterflow.http

import com.typesafe.scalalogging.Logger
import org.json4s
import org.json4s.jackson.Serialization
import org.json4s.{ShortTypeHints, jvalue2extractable}
import org.scalatra.*
import org.scalatra.json.*
import svend.playground.waterflow.{LocalTask, Noop, RunLog, SparkTask, SshTask, Task}

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

class TaskExecutorServlet(executionContext: ExecutionContext) extends ScalatraServlet
  with JacksonJsonSupport
  with FutureSupport {

  val logger = Logger(classOf[TaskExecutorServlet])

  given executor: ExecutionContext = executionContext

  given jsonFormats: json4s.Formats =
    Serialization.formats(ShortTypeHints(List(classOf[LocalTask], classOf[SparkTask], classOf[SshTask]))) +
      json4s.ext.JInstantSerializer

  before() {
    // sets the output content-type to JSON, for all routes
    contentType = formats("json")
  }

  post("/runtask") {
    val RunTaskRequest(task, attempt) = parsedBody.extract[RunTaskRequest]
    logger.info(s"Processing $task")
    new AsyncResult {
      override val is =
        TaskRunner.run(task)
        // TODO: if TaskRunner.run(task) is failed, return an error here, with a log 
          .map(runLog => Ok(RunTaskResponse(runLog, true, attempt)))
    }
  }
}

object TaskRunner {

  def run(task: Task): Future[RunLog] = {
    task match {
      case s: SparkTask => dispatchToRunner(s)
      case s: LocalTask => dispatchToRunner(s)
      case s: SshTask => dispatchToRunner(s)
      case Noop => Future.successful(RunLog(Instant.now(), ""))
    }
  }

  // ----

  // a Runner can run anything, not necessarily in the Task type hierarchy
  trait Runner[T] {
    def run(startTime: Instant, task: T): Future[RunLog]
  }

  /**
   * I'm totally abusing typeclass here just because I wanted an excuse to use them:
   * Looks up a Runner for any T and uses it to execute the task.
   * Since all T are actually instances of Task, using ad-hoc inheritance is probably
   * not the most logical choice (but this is a toy project, I do what I want :) )
   *
   * I think IRL, type classes are meant to be used when all parts of the code are
   * aware of the exact type T, not when they are handled as some common super class of T.
   * */

  private def dispatchToRunner[T](task: T)(using runner: Runner[T]): Future[RunLog] = {
    runner.run(Instant.now(), task)
  }

  // specific logic for running any tasks
  given Runner[LocalTask] with {
    override def run(startTime: Instant, task: LocalTask): Future[RunLog] =
      Future.successful {
        Thread.sleep(300)
        RunLog(startTime, s"${task.label}: user ${task.user} runs ${task.command}")
      }
  }

  given Runner[SshTask] with {
    override def run(startTime: Instant, task: SshTask): Future[RunLog] =
      Future.successful {
        Thread.sleep(600)
        RunLog(startTime, s"${task.label}: user ${task.user} runs ${task.shellCommand} on ${task.host}:${task.port}")
      }
  }

  given Runner[SparkTask] with {
    override def run(startTime: Instant, task: SparkTask): Future[RunLog] =
      Future.successful {
        Thread.sleep(2000)
        RunLog(startTime, s"${task.label}: jobName: ${task.jobName}")
      }
  }

}

case class RunTaskRequest(task: Task, attempt: Int)
case class RunTaskResponse(runLog: RunLog, isSuccess: Boolean, attempt: Int)