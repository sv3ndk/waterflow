package svend.playground

import svend.playground.dag.Dag
import svend.playground.dag.*
import svend.playground.dag.Task.*
import org.scalatest.flatspec.*
import org.scalatest.EitherValues.*
import org.scalatest.matchers.*
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.scalacheck.Gen
import DataGen.*

class DagTest extends AnyFlatSpec with must.Matchers with ScalaCheckPropertyChecks {

  behavior of "A DAG"

  it must "be empty when built with no dependencies" in {

    // EitherValues allows to access an Either that should be right
    val emptyDag: Dag = Dag().value

    assert(emptyDag.isEmpty)
    emptyDag.size must be (0)
  }

  it must "be linearizable and of size n when built with any set of n independent tasks" in {
    forAll(taskListGen) {
      (tasks: Seq[Task]) => {
        val dependencies = tasks.map(Dependency.independent)
        val dag = Dag(dependencies).value

        dag.size must be (tasks.size)
        Dag.linearizable(dag) must be(true)
      }
    }
  }

  it must "be empty when built with only dependencies towards Noop" in {
    // weird case but valid: any Dag with only Noop tasks => should result in empty DAG
    forAll(Gen.chooseNum(0, 2000)) {(dagSize: Int) =>
      val emptyDag = Dag(List.fill(dagSize)(Dependency(Noop, Noop))).value

      assert(emptyDag.isEmpty)
      emptyDag.size must be (0)
      Dag.linearizable(emptyDag) must be(true)
    }
  }


  it must "have size n when built with any sequence of tasks that depend each on the next one" in (pending)

  it must "result in the same DAG when dependencies are provided in a different order" in (pending)

  it must "refuse any set >= 2 of tasks that form a cycle" in (pending)

}
