package svend.playground.waterflow.http

import com.fasterxml.jackson.core.JsonParseException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must

import scala.concurrent.duration.DurationInt
import requests.{RequestFailedException, Response, headers}
import svend.playground.waterflow.{LocalTask, RetryableTaskFailure, RunLog, Task}

import java.nio.charset.Charset
import java.time.Instant
import scala.concurrent.{Await, ExecutionContext, Future}

class RemoteTaskRunnerTest extends AnyFlatSpec with must.Matchers {

  given ec: ExecutionContext = ExecutionContext.Implicits.global

  def testedDispatcher(mockedResult: Future[Response]): RemoteTaskRunner =
    new RemoteTaskRunner("no-url") {
      override def submitTask(task: Task)(using ExecutionContext): Future[Response] = mockedResult
    }

  def okHttpResponse(body: String): Future[Response] =
    Future.successful(
      Response(
        url = "http://anywhere",
        statusCode = 200,
        statusMessage = "ok",
        data = geny.Bytes(body.getBytes(Charset.forName("UTF-8"))),
        headers = Map.empty,
        history = None
      )
    )

  def notOkHttpResponse(body: String, statusCode: Int): Future[Response] =
    Future.failed(
      new RequestFailedException(
        Response(
          url = "http://anywhere",
          statusCode = statusCode,
          statusMessage = "notOk",
          data = geny.Bytes(body.getBytes(Charset.forName("UTF-8"))),
          headers = Map.empty,
          history = None
        )
      )
    )

  val someLocalTask = LocalTask("mkdir -p /home/foo", "foo")

  behavior of "RemoteDispatcher"

  it must "transform correctly a successful result" in {
    val okResponse = okHttpResponse(
      """{"runLog": {"startTime":10,"endTime":20,"logs":"this is fine"},"isSuccess":true} """
    )
    val result = Await.result(testedDispatcher(okResponse).run(someLocalTask), 1.second)
    result mustBe RunLog(
      Instant.ofEpochMilli(10),
      Instant.ofEpochMilli(20),
      "this is fine"
    )
  }

  it must "yield non retryable failure in case of badly formatted response" in {
    val okResponse = okHttpResponse(
      """{{{this is not a valid JSON !!!, "isSuccess":true} """
    )
    assertThrows[JsonParseException] {
      Await.result(testedDispatcher(okResponse).run(someLocalTask), 1.second)
    }
  }

  it must "retry if the isSuccess flag if false" in {
    val notOkResponse = okHttpResponse(
      """{"runLog": {"startTime":10,"endTime":20,"logs":"this not is fine"},"isSuccess":false} """
    )
    assertThrows[RetryableTaskFailure] {
      Await.result(testedDispatcher(notOkResponse).run(someLocalTask), 1.second)
    }
  }

  it must "retry if the HTTP code is 503 Service Unavailable" in {
    // isSuccess is true, but HTTP code is still not ok => retry anyway
    val notOkResponse = notOkHttpResponse(
      """{"runLog": {"startTime":10,"endTime":20,"logs":"this not is fine"},"isSuccess":true} """,
      503
    )
    assertThrows[RetryableTaskFailure] {
      Await.result(testedDispatcher(notOkResponse).run(someLocalTask), 1.second)
    }
  }

  it must "NOT retry if the HTTP code is 400 Bad Request" in {
    // isSuccess is true, but HTTP code is still not ok => retry anyway
    val notOkResponse = notOkHttpResponse(
      """{"runLog": {"startTime":10,"endTime":20,"logs":"this not is fine"},"isSuccess":true} """,
      400
    )
    assertThrows[RequestFailedException] {
      Await.result(testedDispatcher(notOkResponse).run(someLocalTask), 1.second)
    }
  }

}
