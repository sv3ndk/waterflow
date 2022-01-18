package svend.playground.usecases

import svend.playground.dag.{Dag, Dependency}
import svend.playground.dag.Task.*

/**
 * Example of the API usage
 */
object UseCase1 {

  lazy val dag: Either[IllegalArgumentException, Dag] = Dag {

    // tasks
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

}
