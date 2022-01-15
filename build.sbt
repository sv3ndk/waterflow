lazy val waterflow = project
  .in(file("."))
  .settings(
    name := "waterflow",
    scalaVersion := "3.1.0",

    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.10" % "test",
      "org.scalacheck" %% "scalacheck" % "1.15.4" % "test"
    )

  )
