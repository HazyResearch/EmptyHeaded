package duncecap

object IROptimizer {
  def applySubstitutionsToRule(rule:Rule, substitutions:Map[String, String]): Rule = {
    Rule(rule.result,
         rule.recursion,
         rule.operation,
         rule.order,
         rule.project,
         Join(rule.join.rels.map(rel => {
           if (substitutions.contains(rel.name)) {
             Rel(substitutions(rel.name), rel.attrs, rel.anno)
           } else {
             rel
           }
         })),
         rule.aggregations, /* TODO, you may need to substitute in a CONST agg as well */
         rule.filters,
         rule.orderBy)
  }

  def dedupComputationsHelper(rules:List[Rule],
                              seen:List[Rule],
                              substitutions:Map[String, String]): List[Rule] = {
    val cur = applySubstitutionsToRule(rules.head, substitutions)
    val seenRule = seen.find(rule => cur.attrNameAgnosticEquals(rule))
    if (seenRule.isEmpty && !rules.tail.isEmpty) {
      cur::dedupComputationsHelper(rules.tail, cur::seen, substitutions)
    } else if  (seenRule.isEmpty && seenRule.isEmpty) {
      List(cur)
    } else {//seenRule has something
      if (cur.result.isIntermediate) {
        if (rules.tail.isEmpty) {
          List()
        } else {
          dedupComputationsHelper(
            rules.tail,
            seen,
            substitutions + (cur.result.rel.name -> seenRule.get.result.rel.name))
        }
      } else {
        val rewriteCur = Rule(
          cur.result,
          None,
          Operation("*"),
          Order(cur.result.rel.attrs),
          Project(Attributes(List())),
          Join(List(seenRule.get.result.rel)),
          Aggregations(List()),
          Filters(List())
        )
        if (rules.tail.isEmpty) {
          rewriteCur::List()
        } else {
          rewriteCur::dedupComputationsHelper(
            rules.tail,
            seen,
            substitutions)
        }
      }
    }
  }
  
  def dedupComputations(ir:IR): IR = {
    IR(dedupComputationsHelper(ir.rules, List(), Map()))
  }

  // SQL aggregations need to be mapped on to the AJAR model, sometimes in special ways. E.g., the expression inside a
  // SQL aggregation doesn't touch columns in every relation, so we don't actually want to aggregate over every
  // relation.
  def setSQLAnnotations(ir:IR) = {

    def allSubtreeRels(rule:Rule):List[String] = {
      val joinRelNames = rule.join.rels.map(_.name)
      val children = ir.rules.filter(rule => joinRelNames.contains(rule.result.rel.name))
      joinRelNames ::: children.flatMap(allSubtreeRels)
    }

    // Create a new set of rules with no aggregations or annotations.
    val result = IR(ir.rules.map(rule => {
      val newRule = Rule(
        Result(Rel(rule.result.rel.name, rule.result.rel.attrs), rule.result.isIntermediate),
        rule.recursion,
        rule.operation,
        rule.order,
        rule.project,
        Join(rule.join.rels.map(oldRel => Rel(oldRel.name, oldRel.attrs))),
        Aggregations(List()),
        rule.filters,
        rule.orderBy
      )

      newRule
    }))

    ir.rules.zip(result.rules).foreach({
      case (oldRule, newRule) => {
        val baseAggregations = oldRule.aggregations.values.flatMap(agg => {
          // We need all of the involved attrs. to be in current bag's subtree. If they aren't this aggregation can't
          // be computed, and shouldn't show up on the new rules.
          val allInvolvedAttrsBelow = agg.innerExpression.involvedRelations.toSet.subsetOf(allSubtreeRels(oldRule).toSet)

          if (!allInvolvedAttrsBelow) {
            None
          } else {
            // Check that the aggregation hasn't already been added to new rules. This works because rules are in a
            // bottom up order. If it has already been added, we can rewrite the inner expression to just be the
            // annotation name.
            val alreadyAdded = result.rules.exists(
              rule => rule.aggregations.values.map(_.annotation).contains(agg.annotation)
            )

            if (alreadyAdded) {
              agg.innerExpression = LiteralExpr(agg.annotation, agg.innerExpression.involvedRelations)
            } else {
              // If it hasn't already been added, add the annotations to the tables that are joined in (the base tables,
              // not intermediate bags we create). This might require going down the tree if some relations are lower.
              agg.innerExpression.relationsToColumn.foreach({
                case (relationName, columns) => {
                  val relation = result.rules.flatMap(_.join.rels).find(_.name == relationName).get
                  relation.anno = Annotations((relation.anno.values ::: columns).distinct)
                }
              })
            }
            Some(agg)
          }
        })
        newRule.aggregations.values = baseAggregations
      }
    })

    // Set annotations appropriately.
    result.rules.zipWithIndex.foreach({
      case (rule, i) => {
        // The result all of the annotations of the aggregations in the bag, or annotations on lower bags that
        // are joined in (this works because we are going bottom up). The last rule is a special case, because it is what
        // is output, so we take the original annotations.
        rule.result.rel.anno = if (i == result.rules.length - 1) {
          ir.rules.last.result.rel.anno
        } else {
          Annotations(
            if (rule.aggregations.values.nonEmpty) {
              rule.aggregations.values.map(_.annotation)
            } else {
              rule.join.rels.flatMap(_.anno.values).distinct
            }
          )
        }

        // Now, if the result of this bag is joined into any other bags, it should carry its annotations to the parent.
        result.rules.foreach(innerRule => {
          innerRule.join.rels.find(_.name == rule.result.rel.name) match {
            case Some(upperRel) => {
              upperRel.anno = rule.result.rel.anno
            }
            case None =>
          }
        })
      }
    })

    result
  }

  // There is currently one query where the topdown pass is disconnected (i.e. it would require a union because the
  // relations in the topdown pass form multiple connected components). This is a hack to fix that, by finding another
  // relation that normally wouldn't be included in the topdown pass to link the components together.
  def fixTopDownPass(ir:IR) = {
    val topDownRelations = ir.rules.last.join.rels
    val otherRelations = ir.rules.dropRight(1).flatMap(_.join.rels)

    if (topDownRelations.length > 1) {
      topDownRelations.find(
        rel => !topDownRelations.exists(
          otherRel => rel != otherRel && rel.attrs.values.intersect(otherRel.attrs.values).nonEmpty
        )
      ) match {
        case Some(disconnectedRel) => {
          val otherTopDownRelations = topDownRelations.filter(_ != disconnectedRel)
          val joiningRelation = otherRelations.find(rel => {
            rel.attrs.values.intersect(disconnectedRel.attrs.values).nonEmpty &&
              otherTopDownRelations.exists(otherTopDownRel => {
                rel.attrs.values.intersect(otherTopDownRel.attrs.values).nonEmpty
              })
          }).get
          ir.rules.last.join.rels = joiningRelation :: ir.rules.last.join.rels
        }
        case None =>
      }
    }
  }
}
