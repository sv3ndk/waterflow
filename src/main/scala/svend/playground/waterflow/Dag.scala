package svend.playground.waterflow

import com.typesafe.scalalogging.Logger

import scala.annotation.tailrec
import scala.collection.MapView
import scala.util.{Failure, Success, Try}


enum Task(val label: String) {

  case LocalTask(command: String, user: String) extends Task("Local shell task")
  case SparkTask(jobName: String, master: String, mainClass: String, numCores: Int) extends Task("Spark job task")
  case SshTask(host: String, port: Int, user: String, shellCommand: String) extends Task("Remote task over SSH")

  /**
   * Task that does nothing, used as upstream of independent tasks.
   * */
  case Noop extends Task("Null task, not doing anything")

  infix def >>(downstream: Task): Dependency = Dependency(this, downstream)

}

/**
 * Describes that upstream must be executed before downstream
 */
case class Dependency(upstream: Task, downstream: Task)

object Dependency {

  def independent(task: Task) = Dependency(Task.Noop, task)
}

/**
 * A DAG is defined as a map from all tasks to be executed associated to
 * the list of their upstream dependencies.
 */
case class Dag private(tasksWithUpstreams: Map[Task, Set[Task]]) {

  def tasks: Set[Task] = tasksWithUpstreams.keys.toSet

  /**
   * @return the tasks that currently have no dependencies => can be executed
   */
  def freeTasks: Set[Task] =
    tasksWithUpstreams
      .filter { case (task, upstreams) => upstreams.isEmpty }
      .keys
      .toSet

  /**
   * @return one of the currently free tasks
   */
  def aFreeTask(): Option[Task] = freeTasks.headOption

  /**
   * @return a copy of this DAG with all free tasks removed
   */
  def withFreeTasksRemoved: Dag = {
    val remainingTasks: Map[Task, Set[Task]] = for {
      (task, upstreams) <- tasksWithUpstreams
      if upstreams.nonEmpty
    } yield (task -> (upstreams diff freeTasks))
    Dag(remainingTasks)
  }

  /**
   * @return a copy of this DAG with that task removed
   */
  def withTaskRemoved(removedTask: Task): Dag =
    Dag(tasksWithUpstreams
      .removed(removedTask)
      .view.mapValues(upstreams => upstreams - removedTask)
      .toMap
    )

  /**
   * @return the set of tasks that this task depends on
   */
  def dependencies(task: Task): Set[Task] = tasksWithUpstreams(task)

  export tasksWithUpstreams.isEmpty
  export tasksWithUpstreams.size
}

object Dag {

  val logger = Logger(classOf[Dag.type])

  /**
   * Builds a DAG out of a List of Task dependencies.
   * Any Noop Task will not included.
   */
  def apply(deps: Seq[Dependency] = Nil): Try[Dag] = {

    logger.info(s"Building a DAG from ${deps.size} dependencies")

    // tasks and their upstream tasks, for all tasks having at least one upstream
    val tasksWithUpstream: Map[Task, Set[Task]] = deps
      .groupBy(_.downstream)
      .filter((task, _) => task != Task.Noop)
      .view.mapValues(deps => deps.map(_.upstream).filter(_ != Task.Noop).toSet)
      .toMap

    // same as above, augmented with any independent "root" upstream task
    val allTasksWithUpstream: Map[Task, Set[Task]] = deps
      .map(_.upstream)
      .filter(_ != Task.Noop)
      .foldLeft(tasksWithUpstream) {
        case (acc, task) => if acc.contains(task) then acc else acc + (task -> Set.empty)
      }

    val dag = this (allTasksWithUpstream)
    if (linearizable(dag))
      Success(dag)
    else
      Failure(new IllegalArgumentException("The task dependencies contain cycles"))
  }

  def apply(singleDep: Dependency): Try[Dag] = {
    this (List(singleDep))
  }

  /**
   * Validates that the dependencies between those tasks allow to linearize them, i.e. there
   * exists at least one sequencial scheduling of the tasks that respects all dependencies.
   */
  @tailrec
  def linearizable(dag: Dag): Boolean =
    if (dag.isEmpty) true
    else if (dag.freeTasks.isEmpty) false
    else linearizable(dag.withFreeTasksRemoved)

}
