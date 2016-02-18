package duncecap


case class UnsupportedEvalGraphException(what:String) extends Exception(what)

object EvalGraph {
  def isRecursive(statement:ASTQueryStatement) = {
    statement.dependsOn(statement)
  }

  def computeDependencyGraph(statements: List[ASTQueryStatement]): EvalGraphNode = {
    // one ASTStatement could be dependent on another if it has the other's output
    // in it's list of input rels, or in its annotation
    val (start, left) = if (EvalGraph.isRecursive(statements.head)) {
      val otherPart = statements.tail.find(st => st.lhs.name == statements.head.lhs.name)
      if(otherPart.isEmpty) {
        throw InfiniteRecursionException("")
      }
      (EvalGraphNode(statements.head, otherPart), statements.tail.filter(st => st.lhs.name != statements.head.lhs.name))
    } else {
      (EvalGraphNode(statements.head, None), statements.tail)
    }

    val (deps, notDeps) = left.partition(statement => start.dependsOn(statement))
    val notDepsSet = notDeps.toSet
    val depsSet = deps.toSet
    start.deps = deps.flatMap(dep => {
      /* hack to get this to work for now, obviously not general */
      Some(EvalGraphNode(dep, None))
    })
    if (start.deps.size > 1) {
      throw UnsupportedEvalGraphException("only one dep is supported right now")
    }
    return start
  }
}

case class EvalGraph(val statements:List[ASTQueryStatement]) {
  val dependencyGraph = EvalGraph.computeDependencyGraph(statements.reverse)
  def computePlan(config:Config): List[QueryPlan] = {
    dependencyGraph.computePlan(config).reverse
  }
}

case class EvalGraphNode(val statement:ASTQueryStatement, val baseCase:Option[ASTQueryStatement]) {
  var deps: List[EvalGraphNode] = List()
  def computePlan(config:Config): List[QueryPlan] = {
    val thisPlan = if (baseCase.isDefined) {
      val recursion = (new Recursion(RecursionNode(statement.join, statement.joinAggregates, statement.lhs, Some(statement.convergence.get)),
        RecursionNode(baseCase.get.join, baseCase.get.joinAggregates, baseCase.get.lhs, None)))
      recursion.doPostProcessingPass
      recursion.getQueryPlan
    } else {
      statement.computePlan(config, false)
    }
    thisPlan::deps.flatMap(_.computePlan(config)).toList
  }

  def dependsOn(other:ASTQueryStatement) = {
    statement.dependsOn(other) || (baseCase.isDefined && baseCase.get.dependsOn(other))
  }
}