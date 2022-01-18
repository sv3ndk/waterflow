package svend.playground.dag

import scala.annotation.tailrec
import scala.collection.MapView

/**
 * Container of the tree of operators to execute
 */
case class Dag private(tasksWithUpstreams: Map[Task, List[Task]]) {

  /** list of task that currently have no dependencies */
  def freeTasks: List[Task] = {
    tasksWithUpstreams
      .filter { case (task, upstreams) => upstreams.isEmpty }
      .keys
      .toList
  }

  /** Remove all free tasks from that Dag, both as a node and a dependency */
  def withFreeTasksRemoved: Dag = {

    val remainingTasks: Map[Task, List[Task]] = tasksWithUpstreams
      .filter { case (task, upstreams) => upstreams.nonEmpty }
      .map { case (task, upstream) => (task, upstream diff freeTasks) }

    Dag(remainingTasks)
  }

  export tasksWithUpstreams.isEmpty
  export tasksWithUpstreams.size
}

object Dag {

  /**
   * Builds a Dag out of a List of Task dependencies
   */
  def apply(deps: => List[Dependency] = Nil): Either[IllegalArgumentException, Dag] = {

    import Task.Noop

    // task and their upstream task, not including orphans (i.e. tasks without upstream)
    val tasksWithUpstream: Map[Task, List[Task]] = deps
      .groupBy(_.toTask)
      .view.mapValues(deps => deps.map(dep => dep.fromTask).filter(_ != Noop))
      .toMap

    // same as above, but including any orphans (which can only potentially come from the "from" part)
    val allTasksWithUpstream: Map[Task, List[Task]] = deps
      .map(_.fromTask)
      .filter(_ != Noop)
      .foldLeft(tasksWithUpstream) {
        case (acc, task) => if acc.contains(task) then acc else acc + (task -> List())
      }

    val dag = this (allTasksWithUpstream)
    if (linearizable(dag))
      Right(dag)
    else
      Left(new IllegalArgumentException("The task dependencies contain cycles"))
  }

  def apply(singleDep: Dependency): Either[IllegalArgumentException, Dag] = {
    this(List(singleDep))
  }

  /**
   * Validates that the dependencies between those tasks allow to linearize them, i.e.
   */
  @tailrec
  def linearizable(dag: Dag): Boolean = {
    if (dag.isEmpty) true
    else if (dag.freeTasks.isEmpty) false
    else linearizable(dag.withFreeTasksRemoved)
  }

}

enum Task(val label: String) {

  case LocalTask(l: String, command: String) extends Task(l)
  case SparkTask(l: String, submitCommand: String) extends Task(l)
  case SshTask(l: String, shellCommand: String) extends Task(l)

  /**
   * Task that does nothing, handy for communicating tasks with no upstream.
   * */
  case Noop extends Task("noop")

  def >>(downStreamOperator: Task): Dependency = Dependency(this, downStreamOperator)

}

case class Dependency(fromTask: Task, toTask: Task)
