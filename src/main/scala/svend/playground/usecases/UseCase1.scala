package svend.playground.usecases

import svend.playground.Dag
import svend.playground.Operator.*

object UseCase1 {

  // this creates this kind of structure:

  // LocalOperator(hello - 11,ls)
  //  SparkOperator(hello 21,ls)
  //    LocalOperator(hello 31,ls)
  //  LocalOperator(hello 22,ls)
  //    SshOperator(hello 32,ls)

  lazy val dag = {

    // technically, this returns hello 11, with its children wrapped
    val b1 = LocalOperator("hello - 11", "ls") >>: SparkOperator("hello 21", "ls") >>: LocalOperator("hello 31", "ls")

    // to this attaches one more child, hello 22 and its own child, to hello 11
    val b2 = b1 >>: LocalOperator("hello 22", "ls") >>: SshOperator("hello 32", "ls")

    b2
  }


}
