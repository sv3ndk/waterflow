package svend.playground.dag

import scala.collection.MapView
import scala.annotation.tailrec

/**
 * Container of the tree of operators to execute
 *
 */

case class Dag private(sortedTasks: List[Task])

object Dag {

  def apply(deps: => List[Dependency]): Dag = {

    // task and their upstream task, not including orphans (i.e. taks without upstream)
    val tasksWithUpstream: Map[Task, List[Task]] = deps
      .groupBy(_.toTask)
      .view.mapValues(deps => deps.map(dep => dep.fromTask))
      .toMap

    // same as above, but including any orphans
    val allTasksWithUpstream: Map[Task, List[Task]] = deps
      .map(_.fromTask)
      .foldLeft(tasksWithUpstream) {
        case (acc, task) => if acc.contains(task) then acc else acc + (task -> List())
      }

    // provide an order for executing the tasks that respects the dependencies
    @tailrec def linearize(acc: List[Task], remaining: Map[Task, List[Task]]): List[Task] = {

      if (remaining.isEmpty)
        acc
      else {

        val (noUpstream, blockedTask) = remaining.partition {
          case (_, upstreams) => upstreams.isEmpty
        }

        // TODO: if freeTasks is empty here, we're not making progress, and it means there are cycles
        // in the dependencies => should blow up

        val freeTasks = noUpstream.keys.toList
        val stillBlocked = blockedTask.map { case (task, upstream) => (task, upstream diff freeTasks) }

        linearize(acc ++ freeTasks, stillBlocked)

      }
    }

    Dag(linearize(List.empty, allTasksWithUpstream))
  }
}


enum Task(val label: String) {

  case LocalTask(l: String, command: String) extends Task(l)
  case SparkTask(l: String, submitCommand: String) extends Task(l)
  case SshTask(l: String, shellCommand: String) extends Task(l)

  private[dag] case RootTask extends Task("root")

  def >>(downStreamOperator: Task): Dependency = Dependency(this, downStreamOperator)

}

case class Dependency(fromTask: Task, toTask: Task)
