package svend.playground.waterflow

import svend.playground.waterflow.Task.{LocalTask, Noop, SparkTask, SshTask}
import svend.playground.waterflow.{Dag, Task}

import java.time.{Instant, LocalDateTime}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * The scheduler is responsible for launching the tasks of a DAG when
 * their dependencies have been executed.
 * */
class Scheduler(val dispatcher: Dispatcher = LocalDispatcher) {

  def run(fullDag: Dag)(using ec: ExecutionContext): Future[Seq[RunLog]] = {

    println(s"Running DAG asynchronously: $fullDag")

    @tailrec
    def doRun(remainingDag: Dag, runningTasks: Map[Task, Future[RunLog]]): Future[Seq[RunLog]] =
      if (remainingDag.isEmpty)
      // All done: wrapping all the scheduled tasks together
        Future.sequence(runningTasks.values).map(_.toSeq.sortBy(_.startTime))

      else {
        remainingDag.aFreeTask() match {
          case Some(freeTask) =>

            // all future tasks that needs to be finished before starting the new one
            val allUpstreams: Set[Future[RunLog]] =
              fullDag.dependencies(freeTask).map(runningTasks)

            // scheduling this one after all its dependencies
            val futureTask: Future[RunLog] =
              Future.sequence(allUpstreams).flatMap(_ => dispatcher.run(freeTask))

            doRun(
              remainingDag.withTaskRemoved(freeTask),
              runningTasks + (freeTask -> futureTask)
            )

          case None =>
            // This can only happen in case of cyclic dependencies, which Dag is responsible for avoiding
            Future.failed(FailedTask(s"This is a bug: no free task anymore. Current DAG: $remainingDag"))
        }
      }

    doRun(fullDag, Map.empty)
  }

}

case class RunLog(startTime: Instant, endTime: Instant, log: String)

object RunLog {
  def apply(startTime: Instant, log: String): RunLog = new RunLog(startTime, Instant.now(), log)
}

case class FailedTask(startTime: Instant, failTime: Instant, log: String) extends RuntimeException

object FailedTask {
  def apply(log: String): FailedTask = new FailedTask(Instant.now(), Instant.now(), log)
}

trait Dispatcher {
  def run(task: Task): Future[RunLog]
}

object LocalDispatcher extends Dispatcher {

  // a Runner can run anything, not necessarily in the Task type hierarchy
  trait Runner[T] {
    def run(startTime: Instant, task: T): Future[RunLog]
  }

  /**
   * I'm abusing typeclass here just because I wanted an excuse to use them:
   * Looks up a Runner for any T and uses it to execute the task.
   * Since all T are actually instances of Task, using ad-hoc inheritance is probably
   * not the most logical choice (but this is a toy project, I do what I want :) )
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

  def run(task: Task): Future[RunLog] = {
    task match {
      case s: SparkTask => dispachToRunner(s)
      case s: LocalTask => dispachToRunner(s)
      case s: SshTask => dispachToRunner(s)
      case Noop => Future.successful(RunLog(Instant.now(), ""))
    }
  }

  private def dispachToRunner[T](task: T)(using runner: Runner[T]): Future[RunLog] = {
    runner.run(Instant.now(), task)
  }

}