package duncecap

object QueryPlanner {
  def findOptimizedPlans(query:String) = {
    val ir = DatalogParser.run(query)

    //This should run the GHD optimizer on any number of rules.
    //I would imagine the optimizer takes in potentially multiple
    //rules for the same relation.
    IR(ir.rules.flatMap(rule => {
      val rootNodes =
      if (!rule.aggregations.values.isEmpty) {
        GHDSolver.computeAJAR_GHD(
          rule.join.rels.map(rel => OptimizerRel.fromRel(rel, rule)).toSet,
          rule.getResult().getRel().getAttributes().toSet,
          rule.getFilters().values.toArray)
      } else {
        GHDSolver.getMinFHWDecompositions(
          rule.join.rels.map(rel => OptimizerRel.fromRel(rel, rule)),
          rule.getFilters().values.toArray,
          None)
      }

      val joinAggregates = rule.getAggregations().values.flatMap(agg => {
        val attrs = agg.attrs.values
        attrs.map(attr => { (attr, agg) })
      }).toMap

      val candidates = rootNodes.map(r =>
        new GHD(
          r,
          rule.join.rels.map(rel => OptimizerRel.fromRel(rel, rule)),
          joinAggregates,
          rule.getResult().getRel(),
          rule.getFilters().values
        ))
      candidates.map(c => c.doPostProcessingPass())

      val chosen = HeuristicUtil.getGHDsWithMaxCoveringRoot(
        HeuristicUtil.getGHDsOfMinHeight(HeuristicUtil.getGHDsWithMinBags(candidates)))
      chosen.head.getQueryPlan()
    }))
  }
}
