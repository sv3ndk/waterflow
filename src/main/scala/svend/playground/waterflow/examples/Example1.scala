package svend.playground.waterflow.examples

import svend.playground.waterflow.{Dag, Dependency, FailedTask, Scheduler}
import svend.playground.waterflow.*
import Example1.dag
import com.typesafe.scalalogging.Logger
import svend.playground.waterflow.http.{RemoteDispatcher, TaskHttpServer}

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Usage example of the task scheduler
 */
object Example1 {

  val logger = Logger(classOf[Example1.type])

  lazy val dag: Try[Dag] = Dag {

    val bobShowsLocalFiles = LocalTask("ls -l", "bob")
    val someSparkJob = SparkTask("Counting words for fun and profit", "http://some-host:1234", "svend.playgound.RunMe", 4)
    val carolFileCopy = LocalTask("cp ~/code/* /mnt/back/2022-01-18/carol", "carol")
    val ensureDanyShellConfig = LocalTask("touch ~/.zshrc", "dany")
    val remoteSshBackup = SshTask("backup-store", 1456, "op", "cat /etc/hosts")
    val aliceSsh = SshTask("snoopy12", 3456, "alice", "echo 'hi wonderland'")

    // DAG is built by enumerating pair-wise dependencies between tasks
    List(
      bobShowsLocalFiles >> someSparkJob,
      someSparkJob >> carolFileCopy,
      ensureDanyShellConfig >> remoteSshBackup,
      Dependency.independent(aliceSsh)
    )
  }

  @main def main(): Unit = {
    logger.info("Starting Waterflow for use case 1")

    val javaExecutor = Executors.newFixedThreadPool(2);
    given ec: ExecutionContext = ExecutionContext.fromExecutor(javaExecutor)

    val port = 8080
    val jettyServer = TaskHttpServer.startEmbeddedServer(port)
    val scheduler = Scheduler(new RemoteDispatcher(s"http://localhost:$port/runtask"))

    dag.foreach {
      theDag =>
        scheduler.run(theDag).onComplete {
          case Success(logs) =>
            logger.info("Dag execution successful: ")
            logs.foreach(runLog => logger.info(runLog.toString))
          case Failure(FailedTask(start, failTime, failedLog)) =>
            logger.info(s"Dag execution failed: $failedLog")
          case Failure(exception) =>
            logger.info(s"Unexpected dag execution failure: $exception")
        }
    }

    logger.info("waiting for all tasks...")
    javaExecutor.awaitTermination(10L, TimeUnit.SECONDS)
    javaExecutor.shutdown()

    logger.info("...done, now shutting down HTTP server.")
    jettyServer.stop()
    jettyServer.join()

    logger.info("Leaving now")
  }

}
