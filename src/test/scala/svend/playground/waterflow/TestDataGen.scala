package svend.playground.waterflow

import org.scalacheck.Gen
import svend.playground.waterflow.*

object TestDataGen {

  object tasks {

    val shellCommandGen: Gen[String] = Gen.oneOf("ls -l", "echo 'hello Waterworld'", "ps -ef", "whoami")

    val linuxUserGen: Gen[String] = for {
      firstLetter <- Gen.alphaChar
      rest <- Gen.alphaNumStr.map(_.take(10))
    } yield (firstLetter +: rest).toLowerCase

    val linuxHostGen: Gen[String] = for {
      firstLetter <- Gen.alphaChar
      rest <- Gen.alphaNumStr.map(_.take(8))
    } yield (firstLetter +: rest).toLowerCase

    val hostPortGen: Gen[Int] = Gen.chooseNum(1024, 9999)

    val localTaskGen: Gen[LocalTask] = for {
      command <- shellCommandGen
      user <- linuxUserGen
    } yield LocalTask(command, user)

    val sparkTaskGen: Gen[SparkTask] = for {
      jobName <- Gen.alphaNumStr.map(_.take(20))
      masterHost <- linuxHostGen
      masterPort <- hostPortGen
      numCores <- Gen.chooseNum(2, 20)
    } yield SparkTask(jobName, s"$masterHost:$masterPort", "org.playground.Main", numCores)

    val sshTaskGen: Gen[SshTask] = for {
      host <- linuxHostGen
      port <- hostPortGen
      user <- linuxUserGen
      shellCommand <- shellCommandGen
    } yield SshTask(host, port, user, shellCommand)

    val taskGen: Gen[Task] = Gen.oneOf(localTaskGen, sparkTaskGen, sshTaskGen)

    val taskListGen: Gen[Seq[Task]] = Gen.listOf(taskGen)
    /**
     * Random list of dependencies forming a linear sequence of tasks
     */
    val sequencialTaskGen: Gen[Seq[Dependency]] = taskListGen
      .map {
        case Nil => Nil
        case firstTask :: rest =>
          rest
            .foldLeft(Seq(Dependency.independent(firstTask))) {
            // chaining all the tasks as depending each on the next one
            case ((dep@Dependency(upStream, downStream)) :: tail, task) => Dependency(downStream, task) :: dep :: tail
          }
          .reverse
      }
    /**
     * Random list of dependencies containing cycles
     */
    val cyclicalDependenciesGen: Gen[Seq[Dependency]] = {
      val withCycles = for {
        deps <- sequencialTaskGen
        if deps.size > 2
        numCycles <- Gen.chooseNum(1, deps.size - 2) // with 3 tasks, we can create 1 cycle
        cycles = deps
          .filter(_.upstream != Noop)
          .sliding(2)
          .map { case List(Dependency(_, down2), Dependency(up1, _)) => Dependency(down2, up1) }
      } yield deps ++ cycles

      // TIL: suchThat is necessary for making sure some conditions of the generator remain true
      // after shrinking.
      withCycles.suchThat(_.size > 2)
    }
  }

  object dags {
    val emptyDag: Dag = Dag().get

    val independentDagGen: Gen[Dag] =
      tasks.taskListGen.map {
        tasks => Dag(tasks.map(Dependency.independent)).get
      }

  }

}