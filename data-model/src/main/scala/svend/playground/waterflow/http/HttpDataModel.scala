package svend.playground.waterflow.http

import svend.playground.waterflow.{RunLog, Task}

case class RunTaskRequest(task: Task)
case class RunTaskResponse(runLog: RunLog, isSuccess: Boolean)