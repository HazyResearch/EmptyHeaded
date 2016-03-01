package duncecap

object QueryPlanner {
  def findOptimizedPlans(ir:IR): IR = {
    // This should run the GHD optimizer on any number of rules.
    // I would imagine the optimizer takes in potentially multiple
    // rules for the same relation.
    IR(ir.rules.foldLeft(List[Rule]())((accum:List[Rule], rule:Rule) => {
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
        )
      )
      candidates.map(candidate => {
        candidate.setJoinAggregates(joinAggregates ++ rule.getAggregations().values.flatMap(agg => { // add the const aggregations
          if (agg.attrs.values.isEmpty) {
            // assignedAttr should be the first attribute that doesn't get materialized
            val assignedAttr = candidate.attributeOrdering.dropWhile(attr => rule.result.rel.attrs.values.contains(attr)).head
            Some((assignedAttr,
              Aggregation(
                agg.annotation,
                agg.datatype,
                agg.operation,
                Attributes(List(assignedAttr)),
                agg.init,
                agg.expression,
                agg.usedScalars)))
          } else {
            None
          }
        }))
      })
      candidates.map(c => c.doPostProcessingPass())

      val filteredCandidates = HeuristicUtil.getGHDsOfMinHeight(HeuristicUtil.getGHDsWithMinBags(candidates))
      val ghdsWithPushedOutSelections = filteredCandidates.map(ghd => new GHD(
        ghd.pushOutSelections(),
        rule.join.rels.map(rel => OptimizerRel.fromRel(rel, rule)),
        ghd.joinAggregates,
        rule.getResult().getRel(),
        rule.getFilters().values
      ))
      ghdsWithPushedOutSelections.map(_.doPostProcessingPass)
      val chosen = HeuristicUtil.getGHDsWithMaxCoveringRoot(HeuristicUtil.getGHDsWithSelectionsPushedDown(
        ghdsWithPushedOutSelections))
      chosen.head.getQueryPlan(accum):::accum
    }).reverse)
  }
}
