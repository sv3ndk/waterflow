package svend.playground.usecases

import svend.playground.Dag
import svend.playground.Operator.*

object UseCase1 {

  lazy val dag = Dag {
    LocalOperator("Hello scala")
  }


}
