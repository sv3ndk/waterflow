package svend.playground

import org.scalacheck.Gen
import svend.playground.dag.*
import svend.playground.dag.Task.*

object DataGen {

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

}
