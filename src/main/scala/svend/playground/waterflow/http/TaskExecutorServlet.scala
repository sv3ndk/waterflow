package svend.playground.waterflow.http

import com.typesafe.scalalogging.Logger
import org.json4s
import org.json4s.MappingException
import org.json4s.jackson.Serialization
import org.json4s.{ShortTypeHints, jvalue2extractable}
import org.scalatra.*
import org.scalatra.json.*
import svend.playground.waterflow.{RetryableTaskFailure, LocalTask, Noop, RunLog, SparkTask, SshTask, Task}

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

/**
 * HTTP entrypoint for the execution of tasks
 */
class TaskExecutorServlet(executionContext: ExecutionContext) extends ScalatraServlet
  with JacksonJsonSupport
  with FutureSupport {

  given executor: ExecutionContext = executionContext

  given jsonFormats: json4s.Formats =
    Serialization.formats(ShortTypeHints(List(classOf[LocalTask], classOf[SparkTask], classOf[SshTask]))) +
      json4s.ext.JInstantSerializer

  // sets the output content-type to JSON, for all routes
  before() {
    contentType = formats("json")
  }

  post("/run") {
    // Scalatra AsyncResult are linked to Servlet async execution
    new AsyncResult {
      override val is =
        Future(parsedBody.extract[RunTaskRequest])
          .flatMap { case RunTaskRequest(task) => FlimsyTaskExecutionLogic.run(task) }
          .map(runLog => Ok(RunTaskResponse(runLog, true)))
          .recover {
            // returning 503 only in case of retryable error 
            case RetryableTaskFailure(startTime, failTime, log) => ServiceUnavailable(log)
            case me: MappingException => BadRequest(s"Invalid input request, ${me.getMessage}")
            case th => InternalServerError(s"Unexpected task executor error, ${th.getMessage}")
          }
    }
  }
}

case class RunTaskRequest(task: Task)
case class RunTaskResponse(runLog: RunLog, isSuccess: Boolean)

/**
 * Fake take execution logic that just randomly succeeds or fails (without actually running anything)
 */
object FlimsyTaskExecutionLogic {

  val logger = Logger(classOf[FlimsyTaskExecutionLogic.type])

  def run(task: Task)(using ExecutionContext): Future[RunLog] = {
    logger.info(s"Processing $task")
    task match {
      case s: SparkTask => dispatchToRunner(s)
      case s: LocalTask => dispatchToRunner(s)
      case s: SshTask => dispatchToRunner(s)
      case Noop => Future.successful(RunLog(Instant.now(), ""))
    }
  }

  // ----

  /**
   * I'm totally abusing typeclass here, just because I wanted an excuse to use them:
   * Looks up a Runner for any T and uses it to execute the task.
   * Since all T are actually instances of Task, using ad-hoc inheritance is probably
   * not the most logical choice (but this is a toy project, I do what I want :) )
   *
   * I think IRL, type classes are meant to be used when all parts of the code are
   * aware of the exact type T, not when they are handled as some common super class of T.
   * */

  // a Runner can run anything, not necessarily in the Task type hierarchy
  trait Runner[T] {
    def run(startTime: Instant, task: T)(using ExecutionContext): Future[RunLog]
  }

  private def dispatchToRunner[T](task: T)(using runner: Runner[T], ec: ExecutionContext): Future[RunLog] = {
    runner.run(Instant.now(), task)
  }

  val rand = Random()

  // specific logic for running any tasks
  given Runner[LocalTask] with {
    override def run(startTime: Instant, task: LocalTask)(using ec: ExecutionContext): Future[RunLog] =
      Future.successful {
        Thread.sleep(rand.between(30, 300))
        RunLog(startTime, s"${task.label}: user ${task.user} runs ${task.command}")
      }.flatMap(runLog =>
        if (rand.nextFloat() > .7) Future.successful(runLog)
        else {
          logger.warn("Simulating an Local runner failure")
          Future.failed(RetryableTaskFailure(startTime, s"Failed to run local task ${task.command} "))
        }
      )
  }

  given Runner[SshTask] with {
    override def run(startTime: Instant, task: SshTask)(using ExecutionContext): Future[RunLog] =
      Future.successful {
        Thread.sleep(rand.between(60, 600))
        RunLog(startTime, s"${task.label}: user ${task.user} runs ${task.shellCommand} on ${task.host}:${task.port}")
      }.flatMap(runLog =>
        if (rand.nextFloat() > .6) Future.successful(runLog)
        else {
          logger.warn("Simulating an SSH runner failure")
          Future.failed(RetryableTaskFailure(startTime, s"Failed to run SSH task ${task.shellCommand} on host ${task.host}:${task.port}"))
        }
      )
  }

  given Runner[SparkTask] with {
    override def run(startTime: Instant, task: SparkTask)(using ExecutionContext): Future[RunLog] =
      Future.successful {
        Thread.sleep(rand.between(200, 2000))
        RunLog(startTime, s"${task.label}: jobName: ${task.jobName}")
      }.flatMap(runLog =>
        if (rand.nextFloat() > .6) Future.successful(runLog)
        else {
          logger.warn("Simulating a Spark job failure")
          Future.failed(RetryableTaskFailure(startTime, s"Failed to run Spark job ${task.jobName} on master ${task.master}"))
        }
      )
  }

}