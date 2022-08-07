lazy val waterflow = project
  .in(file("."))
  .settings(
    name := "waterflow",
    scalaVersion := "3.1.3",

    libraryDependencies ++= Seq(
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "ch.qos.logback" % "logback-classic" % "1.2.11",

      "org.scalatest" %% "scalatest" % "3.2.13" % "test",
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test"

    )



  )
