package DunceCap

abstract trait ASTStatement {
}

//input to this should be 
//(1) list of attrs in the output, 
//(2) list of attrs eliminated (aggregations), 
//(3) list of relations joined
//(4) list of attrs with selections
//(5) list of exressions for aggregations
class ASTLambdaFunction(val inputArgument:QueryRelation,
                        val join:List[QueryRelation],
                        val aggregates:Map[String,ParsedAggregate])

case class ASTQueryStatement(
                              lhs:QueryRelation,
                              joinType:String,
                              join:List[QueryRelation],
                              recursion:Option[RecursionStatement],
                              tc:Option[TransitiveClosureStatement],
                              joinAggregates:Map[String,ParsedAggregate]) extends ASTStatement {
  // TODO (sctu) : ignoring everything except for join, joinAggregates for now

  def computePlan(nprrOnly:Boolean): QueryPlan = {
    // get the annotations
    val missingRelations = join.filter(rel => !Environment.isLoaded(rel))
    if (!missingRelations.isEmpty) {
      throw new RelationNotFoundException("TODO: fill in with a better explanation")
    }
    val joinQueryWithAnnotations = join.map(rel => Environment.setAnnotationAccordingToConfig(rel))

    if (!nprrOnly) {
      val rootNodes = GHDSolver.getMinFHWDecompositions(join);
      val candidates = rootNodes.map(r => new GHD(r, join, joinAggregates, lhs));
      candidates.map(c => c.doPostProcessingPass())
      val chosen = HeuristicUtils.getGHDsWithMaxCoveringRoot(
        HeuristicUtils.getGHDsWithMinBags(candidates))
      chosen.head.getQueryPlan
    } else {
      // since we're only using NPRR, just create a single GHD bag
      val oneBag = new GHD(new GHDNode(join) ,join, joinAggregates, lhs)
      oneBag.doPostProcessingPass
      oneBag.getQueryPlan
    }
  }
}
