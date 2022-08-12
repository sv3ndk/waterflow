package svend.playground.waterflow

import com.typesafe.scalalogging.Logger

import java.time.Instant
import scala.annotation.tailrec
import scala.collection.MapView
import scala.util.{Failure, Success, Try}

// not using scala 3 enum here since they don't play well with json4s atm
sealed trait Task(val label: String) {
  infix def >>(downstream: Task): Dependency = Dependency(this, downstream)
}
case class LocalTask(command: String, user: String) extends Task("Local shell task")
case class SparkTask(jobName: String, master: String, mainClass: String, numCores: Int) extends Task("Spark job task")
case class SshTask(host: String, port: Int, user: String, shellCommand: String) extends Task("Remote task over SSH")

/**
 * Task that does nothing, used as upstream of independent tasks.
 * */
case object Noop extends Task("Null task, not doing anything")

/**
 * Describes that upstream must be executed before downstream
 */
case class Dependency(upstream: Task, downstream: Task)

object Dependency {
  def independent(task: Task) = Dependency(Noop, task)
}

/**
 * Logs produced by the execution of a task
 */
case class RunLog(startTime: Instant, endTime: Instant, logs: String)

object RunLog {
  def apply(startTime: Instant, logs: String): RunLog = new RunLog(startTime, Instant.now(), logs)
}

case class RetryableTaskFailure(startTime: Instant, failTime: Instant, logs: String) extends RuntimeException

object RetryableTaskFailure {
  def apply(logs: String): RetryableTaskFailure = this (Instant.now(), logs)

  def apply(failTime: Instant, logs: String): RetryableTaskFailure = new RetryableTaskFailure(Instant.now(), failTime, logs)
}
