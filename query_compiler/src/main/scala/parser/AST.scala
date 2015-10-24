package DunceCap

import net.liftweb.json.DefaultFormats
import net.liftweb.json.Serialization.writePretty

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

  val queryPlan: QueryPlan = getBestPlan()

  private def getBestPlan(): QueryPlan = {
    // We get the candidate GHDs, i.e., the ones of min width
    val missingRelations = join.filter(rel => !Environment.isLoaded(rel))
    if (!missingRelations.isEmpty) {
      throw new RelationNotFoundException("TODO: fill in with a better explanation")
    }
    val joinQueryWithAnnotations = join.map(rel => Environment.setAnnotationAccordingToConfig(rel))
    val rootNodes = GHDSolver.getMinFHWDecompositions(join);
    val candidates = rootNodes.map(r => new GHD(r, join, joinAggregates, lhs));
    candidates.map(c => println(c.root.attrSet));
    candidates.map(c => c.doPostProcessingPass())
    val chosen = HeuristicUtils.getGHDsWithMaxCoveringRoot(
      HeuristicUtils.getGHDsWithMinBags(candidates))
    val queryPlan = chosen.head.getQueryPlan
    return queryPlan
  }
}
