package svend.playground.waterflow

import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

trait Dispatcher {
  def run(task: Task)(using ec: ExecutionContext): Future[RunLog]
}

case class RunLog(startTime: Instant, endTime: Instant, log: String)

object RunLog {
  def apply(startTime: Instant, log: String): RunLog = new RunLog(startTime, Instant.now(), log)
}

case class FailedTask(startTime: Instant, failTime: Instant, log: String) extends RuntimeException

object FailedTask {
  def apply(log: String): FailedTask = new FailedTask(Instant.now(), Instant.now(), log)
}

