package svend.playground.usecases

import svend.playground.dag.Dag
import svend.playground.dag.Task.*

object UseCase1 {

  lazy val dag: Either[IllegalArgumentException, Dag] = Dag {

    val local11 = LocalTask("hello - 11", "ls")
    val spark21 = SparkTask("hello 21", "ls")
    val local31 = LocalTask("hello 31", "ls")

    val local22 = LocalTask("hello 22", "ls")
    val ssh32 = SshTask("hello 32", "ls")

    val ssh4 = SshTask("no deps for me", "boo")

    List(

      local11 >> spark21,
      spark21 >> local31,

      local22 >> ssh32,

      // ssh4 does not depend on anything => declared with upstream Noop
      Noop >> ssh4
    )

  }

}
