package svend.playground.waterflow

import svend.playground.waterflow.{LocalTask, Noop, SparkTask, SshTask}
import svend.playground.waterflow.{Dag, Task}

import java.time.{Instant, LocalDateTime}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import com.typesafe.scalalogging.Logger
import svend.playground.waterflow.http.RemoteTaskRunner

/**
 * The scheduler is responsible for launching the tasks of a DAG when
 * their dependencies have been executed.
 * */
class Scheduler(val taskRunner: TaskRunner) {

  val logger = Logger(classOf[Scheduler])

  def run(fullDag: Dag)(using ec: ExecutionContext): Future[Seq[RunLog]] = {

    logger.info(s"Scheduling execution of DAG: $fullDag")

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
              fullDag.dependencies(freeTask)
                .map(runningTasks)

            // scheduling this one after all its dependencies
            val futureTask: Future[RunLog] =
              Future.sequence(allUpstreams)
                .flatMap(_ => Scheduler.withRetry(5, () => taskRunner.run(freeTask)))

            doRun(
              remainingDag.withTaskRemoved(freeTask),
              runningTasks + (freeTask -> futureTask)
            )

          case None =>
            // This can only happen in case of cyclic dependencies, which Dag is responsible for avoiding
            Future.failed(new RuntimeException(s"This is a bug: no free task anymore. Current DAG: $remainingDag"))
        }
      }

    doRun(fullDag, Map.empty)
  }

}

object Scheduler {

  val logger = Logger(classOf[Scheduler])

  def withRetry[T](maxAttempts: Int, fut: () => Future[T])(using ExecutionContext): Future[T] = {
    fut()
      .recoverWith {
        case RetryableTaskFailure(_, _, logs) if maxAttempts > 0 =>
          logger.warn(s"failed to run task remotely, $maxAttempts left => retrying. $logs")
          withRetry(maxAttempts - 1, fut)
      }
  }

}

trait TaskRunner {
  def run(task: Task)(using ec: ExecutionContext): Future[RunLog]
}
