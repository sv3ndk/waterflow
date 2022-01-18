package svend.playground

import svend.playground.dag.Dag
import svend.playground.dag.*
import org.scalatest.flatspec.*
import org.scalatest.EitherValues.*
import org.scalatest.matchers.*

class DagTest extends AnyFlatSpec with must.Matchers {

  behavior of "A Dag"

  it must "be empty when built with no dependencies" in {

    // EitherValues allows to access an Either that should be right
    val emptyDag: Dag = Dag().value

    assert(emptyDag.isEmpty)
    emptyDag.size must be (0)
  }

  it must "be empty when built with only dependencies towards Noop" in (pending)

  it must "have size n when built with any set of n tasks, each depending on Noop" in (pending)

  it must "have size n when built with any sequence of tasks that depend each on the next one" in (pending)

  it must "result in the same DAG when dependencies are provided in a different order" in (pending)

  it must "refuse any set >= 2 of tasks that form a cycle" in (pending)

}
