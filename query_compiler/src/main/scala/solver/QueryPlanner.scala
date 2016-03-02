package duncecap

import scala.collection.mutable.ListBuffer

object QueryPlanner {
  private def rewriteRecursiveRule(rule:Rule): Rule = {
    val newName = rule.result.rel.name + "_recursive"
    Rule(
      Result(Rel(newName,rule.result.rel.attrs, rule.result.rel.anno), false),
      rule.recursion,
      rule.operation,
      rule.order,
      rule.project,
      rule.join,
      rule.aggregations,
      rule.filters
    )
  }

  private def markStatementAsRecursive(origRule:Rule, rule:Rule): Rule = {
    Rule(
      rule.result,
      origRule.recursion,
      rule.operation,
      rule.order,
      rule.project,
      rule.join,
      rule.aggregations,
      rule.filters
    )
  }

  private def markBaseCasesAsIntermediate(rules:List[Rule], recursiveRels:Set[String]): List[Rule] = {
    rules.map(rule => {
      if (recursiveRels.contains(rule.result.rel.name)) {
        Rule(
          Result(rule.result.rel, true),
          rule.recursion,
          rule.operation,
          rule.order,
          rule.project,
          rule.join,
          rule.aggregations,
          rule.filters
        )
      } else {
        rule
      }
    })
  }

  private def isRecursiveRule(rule:Rule): Boolean = {
    val joinNames = rule.join.rels.map(_.name)
    return joinNames.contains(rule.result.rel.name)
  }

  def findOptimizedPlans(ir:IR): IR = {
    // This should run the GHD optimizer on any number of rules.
    // I would imagine the optimizer takes in potentially multiple
    // rules for the same relation.
    val recursiveRels = new ListBuffer[String]()
    val irRules = ir.rules.foldLeft(List[Rule]())((accum:List[Rule], origRule:Rule) => {
      val isRecursive = isRecursiveRule(origRule)
      val rule = if (isRecursive) {
        recursiveRels += origRule.result.rel.name
        rewriteRecursiveRule(origRule)
      } else {
        origRule
      }

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
      var rules = chosen.head.getQueryPlan(accum)
      rules = if (isRecursive) {
        markStatementAsRecursive(origRule, rules.head)::rules.tail
      } else {
        rules
      }
      rules:::accum
    }).reverse
    val finalRules = markBaseCasesAsIntermediate(irRules, recursiveRels.toSet)
    IR(finalRules)
  }
}
