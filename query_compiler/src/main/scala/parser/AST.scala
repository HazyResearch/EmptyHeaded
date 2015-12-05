package DunceCap

abstract trait ASTStatement
abstract trait ASTConvergenceCondition

case class ASTItersCondition(iters:Int) extends ASTConvergenceCondition
case class ASTEpsilonCondition(eps:Double) extends ASTConvergenceCondition
case class InfiniteRecursionException(what:String) extends Exception

case class ASTQueryStatement(lhs:QueryRelation,
                             convergence:Option[ASTConvergenceCondition],
                             joinType:String,
                             join:List[QueryRelation],
                             joinAggregates:Map[String,ParsedAggregate]) extends ASTStatement {
  // TODO (sctu) : ignoring everything except for join, joinAggregates for now
  def dependsOn(statement: ASTQueryStatement): Boolean = {
    val namesInThisStatement = (join.map(rels => rels.name)
      :::joinAggregates.values.map(parsedAgg => parsedAgg.expressionLeft+parsedAgg.expressionRight).toList).toSet
    namesInThisStatement.find(name => name.contains(statement.lhs.name)).isDefined
  }

  def computePlan(config:Config, isRecursive:Boolean): QueryPlan = {
    val missingRel = join.find(rel => Environment.setAnnotationAccordingToConfig(rel))
    if (missingRel.isDefined) {
      throw RelationNotFoundException(missingRel.get.name)
    }

    if (!config.nprrOnly) {
      val rootNodes = GHDSolver.getMinFHWDecompositions(join)
      val candidates = rootNodes.map(r => new GHD(r, join, joinAggregates, lhs))
      candidates.map(c => c.doPostProcessingPass())
      val chosen = HeuristicUtils.getGHDsWithMaxCoveringRoot(
        HeuristicUtils.getGHDsOfMinHeight(HeuristicUtils.getGHDsWithMinBags(candidates)))
      if (config.bagDedup) {
        chosen.head.doBagDedup
      }
      chosen.head.getQueryPlan
    } else {
      // since we're only using NPRR, just create a single GHD bag
      val oneBag = new GHD(new GHDNode(join) ,join, joinAggregates, lhs)
      oneBag.doPostProcessingPass
      oneBag.getQueryPlan
    }
  }
}
