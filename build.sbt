lazy val waterflow = project
  .in(file("."))
  .settings(
    name := "waterflow",
    scalaVersion := "3.1.0",

    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.10" % "test",
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test"
    )

  )
