package duncecap

import duncecap.attr.Attr

class GHD(val root:GHDNode,
          val queryRelations:List[OptimizerRel],
          var joinAggregates:Map[String, List[Aggregation]],
          val outputRelation:Rel,
          val selections:List[Selection]) extends QueryPlanPostProcessor {
  val attributeOrdering: List[Attr] = AttrOrderingUtil.getAttributeOrdering(root, queryRelations, outputRelation, selections)
  var depth: Int = -1
  var numBags: Int = -1
  var bagOutputs:List[OptimizerRel] = null
  var attrToRels: Map[Attr, List[OptimizerRel]] = null
  var attrToAnnotation:Map[Attr, String] = null

  private def getAttrsToRelationsMap(): Map[Attr, List[OptimizerRel]] = {
    PlanUtil.createAttrToRelsMapping(attributeOrdering.toSet, bagOutputs)
  }

  def getBagOutputRelations(node:GHDNode) : List[OptimizerRel] = {
    node.outputRelation::node.children.flatMap(child => getBagOutputRelations(child))
  }

  def setJoinAggregates(joinAggregates:Map[String, List[Aggregation]]) = {
    this.joinAggregates = joinAggregates
  }

  /**
   * Do a post-processiing pass to fill out some of the other vars in this class
   * You should call this before calling getQueryPlan
   */
  def doPostProcessingPass() = {
    root.computeDepth
    depth = root.depth
    numBags = root.getNumBags()
    root.setAttributeOrdering(attributeOrdering)

    val attrNames = root.attrSet.toList.sortBy(attributeOrdering.indexOf(_)).mkString("_")

    root.setBagName(outputRelation.name)
    root.setDescendantNames(1, outputRelation.name)

    root.recursivelyComputeProjectedOutAttrsAndOutputRelation(
      if (outputRelation.anno.values.isEmpty) None else Some(outputRelation.anno.values),
      outputRelation.attrs.values,
      outputRelation.attrs.values.toSet
    )

    if (needTopDownPass()) {
      root.setBagName(s"""${outputRelation.name}_root""")
    }
    bagOutputs = getBagOutputRelations(root)
  }

  def pushOutSelections() = {
    root.recursivelyPushOutSelections(outputRelation.attrs.values.toSet)
  }

  def getQueryPlan(prevRules:List[Rule], curRule:Rule): List[Rule] = {
    // do a preorder traversal of the GHDNodes to get the query plans
    val plan = root.recursivelyGetQueryPlan(joinAggregates, needTopDownPass(), prevRules)
    val fullPlan = if(needTopDownPass()) {
      getTopDownPass()::plan
    } else {
      plan
    }

    // Tack on the order by to the top rule.
    fullPlan.head.orderBy = curRule.orderBy
    fullPlan
  }

  def needTopDownPass():Boolean = {
    // no need to do the top down pass since the root has all the materialized attrs
    return !outputRelation.attrs.values.forall(root.noChildAttrSet.contains(_))
  }

  def getTopDownPass():Rule = {
    /**
     * TODO:
     * You don't need to include a bag here if it is subsumed by a higher bag,
     * i.e., all the attributes it outputs are also output by a higher bag
     */
    val rootRel = root.getResult(true, joinAggregates).rel
    val relationsInTopDownPass = rootRel::root.getDescendants(outputRelation.attrs, rootRel.attrs.values.toSet, joinAggregates).distinct

    return Rule(
      Result(outputRelation, false),
      None,
      Operation("*"),
      Order(Attributes(attributeOrdering.filter(attr => outputRelation.attrs.values.toSet.contains(attr)))),
      Project(Attributes(List())),
      Join(relationsInTopDownPass),
      Aggregations(List()),
      Filters(List())
    )
  }
}

