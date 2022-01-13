package svend.playground

import svend.playground.usecases.UseCase1

@main def main(): Unit = {
  println("Starting Waterflow")
  Executor.run(UseCase1.dag)
}