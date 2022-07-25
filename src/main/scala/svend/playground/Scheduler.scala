package svend.playground

import svend.playground.dag.Task.{LocalTask, Noop, SparkTask, SshTask}
import svend.playground.dag.{Dag, Task}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}


/**
 * The scheduler is responsible for launching the tasks of a DAG when
 * their dependencies have been executed.
 *  */
class Scheduler {

  def run(dag: Dag): Try[Seq[RunLog]] = {

    println(s"Running DAG: $dag")

    @tailrec
    def doRun(currentDag: Dag, currentLog: Seq[RunLog]): Try[Seq[RunLog]] = {
      if (currentDag.isEmpty)
        Success(currentLog.reverse)
      else
        currentDag.aFreeTask() match {
          case Some(freeTask) =>
            val log = Runner.run(freeTask)
            doRun(currentDag.withTaskRemoved(freeTask), log +: currentLog  )
          case None =>
            Failure(new IllegalArgumentException(s"Could not run dag, no free task anymore. Current log: $currentLog Current DAG: $currentDag"))
        }
    }

    doRun(dag, Nil)

  }

}

case class RunLog(message: String)

/**
 * I'm abusing typeclass here just because I wanted an excuse to use them:
 * Looks up a Runner for any T and uses it to execute the task.
 * Since all T are actually instances of Task, using ad-hoc inheritance is probably
 * not the most logical choice (but this is a toy project, I do what I want :) )
 * */
object Runner {

  // a Runner can run anything, not necessarily in the Task type hierarchy
  trait Runner[T] {
    def run(task: T): RunLog
  }

  // specific logic for running any tasks
  given Runner[SparkTask] with
    def run(task: SparkTask) =
      RunLog(s"Submitting this spark job to the cluster: ${task.label}")

  given Runner[LocalTask] with
    def run(task: LocalTask) =
      RunLog(s"Running local shell command : ${task.label}")

  given Runner[SshTask] with
    def run(task: SshTask) =
      RunLog(s"Running ssh shell command : ${task.label}")

  def run(task: Task): RunLog = {
    task match {
      case s: SparkTask => dispachToRunner(s)
      case s: LocalTask => dispachToRunner(s)
      case s: SshTask => dispachToRunner(s)
      case Noop => RunLog("")
    }
  }

  private def dispachToRunner[T](task: T)(using runner: Runner[T] ) : RunLog = {
    runner.run(task)
  }

}



