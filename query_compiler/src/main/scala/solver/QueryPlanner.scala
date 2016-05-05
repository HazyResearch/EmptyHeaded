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

  def findOptimizedPlans(ir: IR, expectedFHW: Option[Double] = None): IR = {
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
          GHDSolver.computeAJAR_GHD(
            rule.join.rels.map(rel => OptimizerRel.fromRel(rel, rule)).toSet,
            rule.getResult().getRel().getAttributes().toSet,
            rule.getFilters().values.toArray
          )

      var joinAggregates = Map[String, List[Aggregation]]()
      for (agg <- rule.getAggregations().values) {
        val attrs = agg.attrs.values

        for (attr <- attrs) {
            val aggs = joinAggregates.getOrElse(attr, List())
            joinAggregates += (attr -> (agg :: aggs))
        }
      }

      val candidates = rootNodes.map(r =>
        new GHD(
          r,
          rule.join.rels.map(rel => OptimizerRel.fromRel(rel, rule)),
          joinAggregates,
          rule.getResult().getRel(),
          rule.getFilters().values
        )
      )

      candidates.foreach(candidate => {
        var candidateJoinAggregates = joinAggregates
        rule.getAggregations().values.foreach(agg => {
          // add the const aggregations
          if (agg.attrs.values.isEmpty) {
            // assignedAttr should be the first attribute that doesn't get materialized
            val assignedAttr = candidate.attributeOrdering.dropWhile(attr => rule.result.rel.attrs.values.contains(attr)).head
            val constAgg = Aggregation(
              agg.annotation,
              agg.datatype,
              agg.operation,
              Attributes(List(assignedAttr)),
              agg.init,
              agg.expression,
              agg.usedScalars,
              agg.innerExpression
            )
            val updatedAggList = candidateJoinAggregates.getOrElse(assignedAttr, List()) ++ List(constAgg)
            candidateJoinAggregates += (assignedAttr -> updatedAggList)
          }
        })
        candidate.setJoinAggregates(candidateJoinAggregates)
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

      expectedFHW match {
        case Some(e) => {
          val score = chosen.head.root.fractionalScoreTree()
          assert(e >= score, s"Expected $e, got $score.")
        }
        case _ =>
      }

      var rules = chosen.head.getQueryPlan(accum, rule)
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
