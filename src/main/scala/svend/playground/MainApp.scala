package svend.playground

import svend.playground.usecases.UseCase1

/**
 * main executor of a DAG
 */
@main def main(): Unit = {
  println("Starting Waterflow")
  println(UseCase1.dag.toTry.get)
}