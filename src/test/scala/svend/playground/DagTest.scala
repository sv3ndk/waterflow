package svend.playground

import org.scalacheck.{Gen, Shrink}
import org.scalatest.TryValues.*
import org.scalatest.flatspec.*
import org.scalatest.matchers.*
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import svend.playground.DataGen.*
import svend.playground.dag.*
import svend.playground.dag.Task.*

import scala.util.Random

class DagTest extends AnyFlatSpec with must.Matchers with ScalaCheckPropertyChecks {

  behavior of "A DAG"

  it must "be valid and empty when built from no dependencies" in {
    // EitherValues allows to access an Either that should be right
    val emptyDag: Dag = Dag().success.value

    assert(emptyDag.isEmpty)
    emptyDag.size mustBe 0
  }

  it must "be valid and of size n when built from any set of n independent tasks" in {
    forAll(taskListGen) {
      (tasks: Seq[Task]) => {
        val dependencies = tasks.map(Dependency.independent)
        val validDag = Dag(dependencies).success.value

        validDag.size mustBe tasks.size
        Dag.linearizable(validDag) must be(true)
      }
    }
  }

  it must "have size n when built from any sequence of tasks that depend each on the next one" in {
    forAll(sequencialTaskGen) {
      (dependencies: Seq[Dependency]) => {
        val validDag = Dag(dependencies).success.value

        validDag.size mustBe dependencies.size
        Dag.linearizable(validDag) must be(true)
      }
    }
  }

  it must "be empty when built from only dependencies made of Noop" in {
    // weird case but actually valid: any Dag with only Noop tasks => should result in empty DAG
    forAll(Gen.chooseNum(0, 2000)) { (dagSize: Int) =>
      val emptyDag = Dag(List.fill(dagSize)(Dependency(Noop, Noop))).success.value

      assert(emptyDag.isEmpty)
      emptyDag.size mustBe 0
      Dag.linearizable(emptyDag) must be(true)
    }
  }

  it must "result in the same DAG when dependencies are provided in a different order" in {
    forAll(sequencialTaskGen) {
      (dependencies: Seq[Dependency]) => {
        val shuffled = dependencies.sortBy(_ => Random.nextInt())

        Dag(dependencies).success.value mustBe Dag(shuffled).success.value
      }
    }
  }

  // Note to self: nuclear option to solve generated-values post-condition after shrinking:
  // disable shrinking entirely:
  // implicit val noShrink: Shrink[Seq[Dependency]] = Shrink.shrinkAny

  it must "refuse any set >= 2 of tasks that form a cycle" in {
    forAll(cyclicalDependenciesGen) {
      (cyclicDependencies: Seq[Dependency]) =>
        val invalidDag = Dag(cyclicDependencies).failure.exception
        invalidDag.getMessage mustBe "The task dependencies contain cycles"
    }
  }

}
















