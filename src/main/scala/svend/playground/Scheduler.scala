package svend.playground

import svend.playground.dag.{Dag, Task}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

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
            val log = execute(freeTask)
            doRun(currentDag.withTaskRemoved(freeTask), log +: currentLog  )
          case None =>
            Failure(new IllegalArgumentException(s"Could not run dag, no free task anymore. Current log: $currentLog Current DAG: $currentDag"))
        }
    }

    doRun(dag, Nil)

  }

  def execute(task: Task): RunLog = {
    RunLog(s"Executing '${task.label}', which always succeeds and finishes immediately. Details: $task")
  }

}

case class RunLog(message: String)



