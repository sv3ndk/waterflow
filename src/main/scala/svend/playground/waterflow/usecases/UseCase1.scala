package svend.playground.waterflow.usecases

import svend.playground.waterflow.{Dag, Dependency, FailedTask, Scheduler}
import svend.playground.waterflow.Task.*
import UseCase1.dag

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

/**
 * Example of the API usage
 */
object UseCase1 {

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
    
    println("Starting Waterflow for use case 1")
    val scheduler = Scheduler()

    val javaExecutor = Executors.newFixedThreadPool(2);
    given ec: ExecutionContext = ExecutionContext.fromExecutor(javaExecutor)

    dag.foreach {
      theDag =>
        scheduler.run(theDag).onComplete {
          case Success(logs) =>
            println("Dag execution successful: ")
            logs.foreach(println)
          case Failure(FailedTask(start, failTime, failedLog)) =>
            println(s"Dag execution failed: $failedLog")
          case Failure(exception) =>
            println(s"Unexpected dag execution failure: $exception")
        }
    }

    println("waiting for all tasks...")
    javaExecutor.awaitTermination(5L, TimeUnit.SECONDS)
    javaExecutor.shutdown()

    println("...done, exiting.")

  }

}
