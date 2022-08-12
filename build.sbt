ThisBuild / scalaVersion := "3.1.3"

val loggingLibs = Seq(
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "ch.qos.logback" % "logback-classic" % "1.2.11"
)

val jsonLibs = Seq(
  "org.json4s" %% "json4s-jackson" % "4.0.5",
  "org.json4s" %% "json4s-ext" % "4.0.5"
)

val testLibs = Seq(
  "org.scalatest" %% "scalatest" % "3.2.13" % "test",
  "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % "test"
)

lazy val dataModel = project
  .in(file("data-model"))
  .settings(
    name := "waterflow-data-model",
    description := "Common data models",
    libraryDependencies ++= loggingLibs
  )

lazy val scheduler = project
  .in(file("scheduler"))
  .dependsOn(dataModel)
  .settings(
    name := "waterflow-scheduler",
    description := "Taks scheduler",

    libraryDependencies ++= Seq(
      // dead simple blocking HTTP client
      "com.lihaoyi" %% "requests" % "0.7.1"
    ),

    libraryDependencies ++= loggingLibs,
    libraryDependencies ++= jsonLibs,
    libraryDependencies ++= testLibs
  )

lazy val worker = project
  .in(file("worker"))
  .dependsOn(dataModel)
  .settings(
    name := "waterflow-worker",
    description := "HTTP server responsible for executing tasks upon request",

    libraryDependencies ++= Seq(
      // At the time of writing this, Scalatra is pretty much the only rest lib
      // that is ready for scala 3.
      "org.scalatra" %% "scalatra" % "3.0.0-M1",
      "org.scalatra" %% "scalatra-json" % "3.0.0-M1",
      "javax.servlet" % "javax.servlet-api" % "3.1.0",
      "org.eclipse.jetty" % "jetty-webapp" % "9.4.6.v20170531"
    ),

    libraryDependencies ++= loggingLibs,
    libraryDependencies ++= jsonLibs,
    libraryDependencies ++= testLibs
  )