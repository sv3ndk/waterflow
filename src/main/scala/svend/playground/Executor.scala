package svend.playground

import svend.playground.dag.Dag

/**
 * Main Executor of any DAG
 */
object Executor {

  def run(dag: Dag): Unit = {
    println("Running this DAG:")
    println(dag)
  }

}
