package duncecap

object QueryPlanner {
  def findOptimizedPlans(ir:IR) = {
    //This should run the GHD optimizer on any number of rules.
    //I would imagine the optimizer takes in potentially multiple
    //rules for the same relation.
    IR(ir.rules.flatMap(rule => {
      val rootNodes = GHDSolver.computeAJAR_GHD(
        rule.join.rels.map(rel => OptimizerRel.fromRel(rel, rule)).toSet,
        rule.getResult().getRel().getAttributes().toSet,
        rule.getFilters().values.toArray)

      val joinAggregates = rule.getAggregations().values.flatMap(agg => {
        val attrs = agg.attrs.values
        attrs.map(attr => { (attr, agg) })
      }).toMap

      val candidates = rootNodes.map(r =>
        new GHD(
          r,
          rule.join.rels.map(rel => OptimizerRel.fromRel(rel, rule)),
          joinAggregates,
          rule.getResult().getRel()))
      candidates.map(c => c.doPostProcessingPass())

      val chosen = HeuristicUtil.getGHDsWithMaxCoveringRoot(
        HeuristicUtil.getGHDsOfMinHeight(HeuristicUtil.getGHDsWithMinBags(candidates)))
      chosen.head.getQueryPlan()
    }).reverse)
  }
}
