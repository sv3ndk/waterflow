package svend.playground

/**
 * Containter of the tree of operators to execute
 */
class Dag(operator: Operator)

object Dag {
  def apply(operator: Operator) = {
    new Dag(operator)
  }
}

enum Operator {
  case LocalOperator(command: String)
  case SparkOperator(submitCommand: String)
  case SshOperator(shellCommand: String)
}
