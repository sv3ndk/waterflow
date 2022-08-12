package svend.playground.waterflow


import org.scalatest.TryValues.*
import org.scalatest.flatspec.{AnyFlatSpec, AsyncFlatSpec}
import org.scalatest.matchers.*
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import svend.playground.waterflow.Task

import java.time.Instant
import scala.concurrent.duration.{Duration, DurationInt}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

// Testing the async scheduler with simple Await on the Future[Seq[RunLogs]] returned by run().
// ScalaTest is supposed to support returning Future[Assertion] using the AsyncFlatSpec
// but it does not seem compatible with ScalaCheckPropertyChecks :(

class SchedulerTest extends AnyFlatSpec with must.Matchers with ScalaCheckPropertyChecks {

  import scala.concurrent.ExecutionContext.Implicits.global

  behavior of "scheduler with successful dispatcher"

  val successfulScheduler = new Scheduler(new SuccessfulTaskRunner())
  val rand = Random()

  it must "successfully do nothing with an empty dag" in {
    val noRunLogs = Await.result(successfulScheduler.run(TestDataGen.dags.emptyDag), Duration(1, "sec"))
    noRunLogs mustBe empty
  }

  // no dependencies in the tasks => all we need if all of them to be scheduled exactly once
  it must "start all tasks provided" in {
    forAll(TestDataGen.dags.independentDagGen) {
      (dag: Dag) => {
        val stringLogs = Await
          .result(successfulScheduler.run(dag), Duration(1, "sec"))
          .map(_.logs)
        stringLogs must have length dag.size
        dag.tasks.foreach {
          // all output must be there, in any order
          task => stringLogs must contain(s"executed task ${task}")
        }
      }
    }
  }

  it must "start a chain of dependant tasks one after the other" in {
    forAll(TestDataGen.tasks.sequencialTaskGen) {

      (dependencies: Seq[Dependency]) => {
        val shuffledDeps = rand.shuffle(dependencies)
        val validDag = Dag(shuffledDeps).success.value
        val stringLogs = Await
          .result(successfulScheduler.run(validDag), Duration(1, "sec"))
          .map(_.logs)

        // this time, the logs are expected to be in the dependency order
        val expectedLogs =
          if (dependencies.isEmpty)
            Seq()
          else
            (dependencies.head.upstream +: dependencies.map { case Dependency(upStream, downStream) => downStream })
              // Noop tasks should be skipped
              .filter(_ != Noop)
              .map(task => s"executed task $task")

        stringLogs mustBe expectedLogs
      }
    }
  }

  class SuccessfulTaskRunner extends TaskRunner {
    override def run(task: Task)(using ec: ExecutionContext) = Future {
      val startTime = Instant.now()
      Thread.sleep(10)
      RunLog(startTime, s"executed task ${task}")
    }
  }

  behavior of "retry mechanism"

  it must "call only once a non failing function" in {
    val tenTimesOk: () => Future[Int] = (1 to 10).map(Future.apply).iterator.next
    val result = Await.result(Scheduler.withRetry(5, tenTimesOk), 1.second)
    result mustBe 1
  }

  it must "retry until success if enough attempts are possible" in {
    val fail9timesThenOk:  () => Future[Int] = (
      (1 to 9).map(_ => Future.failed(RetryableTaskFailure("boom")))
        :+ Future.successful(10)
      ).iterator.next
    val result = Await.result(Scheduler.withRetry(10, fail9timesThenOk), 1.second)
    result mustBe 10
  }

  it must "ultimately fail if not enough attempts are possible" in {
    val fail9timesThenOk:  () => Future[Int] = (
      (1 to 9).map(_ => Future.failed(RetryableTaskFailure("boom")))
        :+ Future.successful(10)
      ).iterator.next
    assertThrows[RetryableTaskFailure] {
      Await.result(Scheduler.withRetry(5, fail9timesThenOk), 1.second)
    }
  }

  it must "must not retry a non retryable error even if enough attempts are available" in {
    val failOnceFatallyThenOk:  () => Future[String] =
      List(Future.failed(new RuntimeException("boom")), Future.successful("yoohoo"))
        .iterator.next
    assertThrows[RuntimeException] {
      Await.result(Scheduler.withRetry(5, failOnceFatallyThenOk), 1.second)
    }
  }

}
