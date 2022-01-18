package svend.playground.dag

import scala.annotation.tailrec
import scala.collection.MapView


enum Task(val label: String) {

  case LocalTask(command: String, user: String) extends Task("Local shell task")
  case SparkTask(jobName: String, master: String, mainClass: String, numCores: Int) extends Task("Spark job task")
  case SshTask(host: String, port: Int, user: String, shellCommand: String) extends Task("Remote task over SSH")

  /**
   * Task that does nothing, necessary for communicating tasks with no upstream.
   * */
  case Noop extends Task("Null task, not doing anything")

  def >>(downstream: Task): Dependency = Dependency(this, downstream)

}

/**
 * Dependency of a downstream task depending on an upstream one
 */
case class Dependency(upstream: Task, downstream: Task)

object Dependency {

  def independent(task: Task) = Dependency(Task.Noop, task)
}

/**
 * A DAG is defined as a map from all tasks to be executed to the list of
 * their upstream dependencies.
 */
case class Dag private(tasksWithUpstreams: Map[Task, Seq[Task]]) {

  /** list of tasks that currently have no dependencies */
  def freeTasks: Seq[Task] =
    tasksWithUpstreams
      .filter { case (task, upstreams) => upstreams.isEmpty }
      .keys
      .toSeq

  /** Copy of this DAG with all free tasks removed */
  def withFreeTasksRemoved: Dag = {

    val remainingTasks: Map[Task, Seq[Task]] = tasksWithUpstreams
      .filter { case (task, upstreams) => upstreams.nonEmpty }
      .map { case (task, upstream) => (task, upstream diff freeTasks) }

    Dag(remainingTasks)
  }

  export tasksWithUpstreams.isEmpty
  export tasksWithUpstreams.size
}

object Dag {

  /**
   * Builds a Dag out of a List of Task dependencies. Noop is never included in the DAG.
   */
  def apply(deps: => Seq[Dependency] = Nil): Either[IllegalArgumentException, Dag] = {

    // tasks and their upstream tasks, for all tasks having at least one upstream
    val tasksWithUpstream: Map[Task, Seq[Task]] = deps
      .groupBy(_.downstream)
      .filter((task, _) => task != Task.Noop)
      .view.mapValues(deps => deps.map(dep => dep.upstream).filter(_ != Task.Noop))
      .toMap

    // same as above, augmented with any independent "root" upstream task
    val allTasksWithUpstream: Map[Task, Seq[Task]] = deps
      .map(_.upstream)
      .filter(_ != Task.Noop)
      .foldLeft(tasksWithUpstream) {
        case (acc, task) => if acc.contains(task) then acc else acc + (task -> Nil)
      }

    val dag = this (allTasksWithUpstream)
    if (linearizable(dag))
      Right(dag)
    else
      Left(new IllegalArgumentException("The task dependencies contain cycles"))
  }

  def apply(singleDep: Dependency): Either[IllegalArgumentException, Dag] = {
    this (List(singleDep))
  }

  /**
   * Validates that the dependencies between those tasks allow to linearize them, i.e. there
   * exists at least one sequencial scheduling of the tasks that respects all dependencies.s
   */
  @tailrec
  def linearizable(dag: Dag): Boolean =
    if (dag.isEmpty) true
    else if (dag.freeTasks.isEmpty) false
    else linearizable(dag.withFreeTasksRemoved)

}

