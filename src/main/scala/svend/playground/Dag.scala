package svend.playground

/**
 * Container of the tree of operators to execute
 *
 * The >>: is not really ideal because it builds the tree directly
 * -> we can place each node at only one place, if we place a
 * node at several place to express "more dependencies", we actually
 * add that node several times :(
 *
 * What we would really want to do is express the dependencies of the node,
 * with redundancies and all, and _then_ build a DAG out of that ...
 *
 */
case class Dag(operator: Operator, children: Set[Dag] = Set.empty) {

  // Dag >>: Dag(this)
  def >>:(parent: Dag): Dag = parent.copy(children = parent.children + this)

  // operator >>: Dag(this)
  def >>:(other: Operator): Dag = Dag(other) >>: this

  def asTreeString(indent: Int = 0): String = {
    val currentNode = (" " * indent) + operator.toString + "\n"
    children.foldLeft(currentNode)((acc, c) => acc + c.asTreeString(indent + 2))
  }

}

enum Operator(val label: String) {

  case LocalOperator(l: String, command: String) extends Operator(l)
  case SparkOperator(l: String, submitCommand: String) extends Operator(l)
  case SshOperator(l: String, shellCommand: String) extends Operator(l)

  // Dag >> operator(this)
  def >>:(dag: Dag): Dag = dag >>: Dag(this)

  // operator >> operator(this)
  def >>:(operator: Operator): Dag = operator >>: Dag(this)

}
