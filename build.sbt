val scalatraVersion = "3.0.0-M1"
val json4sVersion = "4.0.5"

lazy val waterflow = project
  .in(file("."))
  .settings(
    name := "waterflow",
    scalaVersion := "3.1.3",

    libraryDependencies ++= Seq(
      // At the time of writing this, Scalatra is pretty much the only rest lib
      // that is ready for scala 3.
      "org.scalatra" %% "scalatra" % scalatraVersion,
      "org.scalatra" %% "scalatra-json" % scalatraVersion,
      "javax.servlet" %  "javax.servlet-api" % "3.1.0",
      "org.eclipse.jetty" % "jetty-webapp" % "9.4.6.v20170531",

      "org.json4s" %% "json4s-jackson" % json4sVersion,
      "org.json4s" %% "json4s-ext" % json4sVersion,

      // dead simple blocking HTTP client
      "com.lihaoyi" %% "requests" % "0.7.1",

      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
      "ch.qos.logback" % "logback-classic" % "1.2.11",

      "org.scalatest" %% "scalatest" % "3.2.13" % "test",
      "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test"
    )

  )