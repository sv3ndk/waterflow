package svend.playground.waterflow

import svend.playground.waterflow.{LocalTask, Noop, SparkTask, SshTask}

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

object LocalDispatcher extends Dispatcher {

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

  // specific logic for running any tasks
  given Runner[LocalTask] with
    override def run(startTime: Instant, task: LocalTask): Future[RunLog] =
      Future.successful {
        Thread.sleep(100)
        RunLog(startTime, s"${task.label}: user ${task.user} runs ${task.command}")
      }

  given Runner[SshTask] with
    override def run(startTime: Instant, task: SshTask): Future[RunLog] =
      Future.successful {
        Thread.sleep(200)
        RunLog(startTime, s"${task.label}: user ${task.user} runs ${task.shellCommand} on ${task.host}:${task.port}")
      }

  given Runner[SparkTask] with
    override def run(startTime: Instant, task: SparkTask): Future[RunLog] =
      Future.successful {
        Thread.sleep(1000)
        RunLog(startTime, s"${task.label}: jobName: ${task.jobName}")
      }

  override def run(task: Task)(using ec: ExecutionContext): Future[RunLog] = {
    task match {
      case s: SparkTask => dispatchToRunner(s)
      case s: LocalTask => dispatchToRunner(s)
      case s: SshTask => dispatchToRunner(s)
      case Noop => Future.successful(RunLog(Instant.now(), ""))
    }
  }

  private def dispatchToRunner[T](task: T)(using runner: Runner[T]): Future[RunLog] = {
    runner.run(Instant.now(), task)
  }

}
