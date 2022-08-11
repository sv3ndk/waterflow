package svend.playground.waterflow

import svend.playground.waterflow.{LocalTask, Noop, SparkTask, SshTask}
import svend.playground.waterflow.{Dag, Task}

import java.time.{Instant, LocalDateTime}
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}
import com.typesafe.scalalogging.Logger

/**
 * The scheduler is responsible for launching the tasks of a DAG when
 * their dependencies have been executed.
 * */
class Scheduler(val dispatcher: Dispatcher = LocalDispatcher) {

  val logger = Logger(classOf[Scheduler.type])

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