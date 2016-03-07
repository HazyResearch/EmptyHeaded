package duncecap

import scala.collection.mutable.ListBuffer

object QueryPlanner {
  private def renameRecursiveBaseCase(rel:Rel): Rel = {
    val newName = rel.name + "_basecase"
    Rel(newName,rel.attrs, rel.anno)
  }

  private def rewriteRecursiveRuleBaseCases(rules:List[Rule], recursiveRels:Set[String]): List[Rule] = {
    rules.map(rule => {
      if (recursiveRels.contains(rule.result.rel.name)) {
        if (isRecursiveRule(rule)) {
          Rule(
            rule.result,
            rule.recursion,
            rule.operation,
            rule.order,
            rule.project,
            Join(rule.join.rels.map(rel => {
              if (rel.name == rule.result.rel.name) {
                renameRecursiveBaseCase(rel)
              } else {
                rel
              }
            })),
            rule.aggregations,
            rule.filters
          )
        } else {
          Rule(
            Result(renameRecursiveBaseCase(rule.result.rel), true),
            rule.recursion,
            rule.operation,
            rule.order,
            rule.project,
            rule.join,
            rule.aggregations,
            rule.filters
          )
        }
      } else {
        rule
      }
    })
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

  private def isRecursiveRule(rule:Rule): Boolean = {
    val joinNames = rule.join.rels.map(_.name)
    return joinNames.contains(rule.result.rel.name)
  }

  def findOptimizedPlans(ir:IR): IR = {
    // This should run the GHD optimizer on any number of rules.
    // The optimizer takes in potentially multiple
    // rules for the same relation.
    val recursiveRels = new ListBuffer[String]()
    val irRules = ir.rules.foldLeft(List[Rule]())((accum:List[Rule], rule:Rule) => {
      val isRecursive = isRecursiveRule(rule)
      if (isRecursive) {
        recursiveRels += rule.result.rel.name
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
        markStatementAsRecursive(rule, rules.head)::rules.tail
      } else {
        rules
      }
      rules:::accum
    }).reverse
    val finalRules  = rewriteRecursiveRuleBaseCases(irRules, recursiveRels.toSet).distinct
    IR(finalRules)
  }
}
