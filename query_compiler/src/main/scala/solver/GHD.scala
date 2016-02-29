package duncecap

import duncecap.attr.Attr

class GHD(val root:GHDNode,
          val queryRelations:List[OptimizerRel],
          val joinAggregates:Map[String, Aggregation],
          val outputRelation:Rel,
          val selections:List[Selection]) extends QueryPlanPostProcessor {
  val attributeOrdering: List[Attr] = AttrOrderingUtil.getAttributeOrdering(root, queryRelations, outputRelation, selections)
  var depth: Int = -1
  var numBags: Int = -1
  var bagOutputs:List[OptimizerRel] = null
  var attrToRels: Map[Attr, List[OptimizerRel]] = null
  var attrToAnnotation:Map[Attr, String] = null
  var lastMaterializedAttr:Option[Attr] = None
  var nextAggregatedAttr:Option[Attr] = None

  private def getAttrsToRelationsMap(): Map[Attr, List[OptimizerRel]] = {
    PlanUtil.createAttrToRelsMapping(attributeOrdering.toSet, bagOutputs)
  }

  def getBagOutputRelations(node:GHDNode) : List[OptimizerRel] = {
    node.outputRelation::node.children.flatMap(child => getBagOutputRelations(child))
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
      if (outputRelation.anno.values.isEmpty) "" else outputRelation.anno.values.head,
      outputRelation.attrs.values.toSet,
      outputRelation.attrs.values.toSet)
    if (needTopDownPass()){
      root.setBagName(s"""${outputRelation.name}_root""")
    }
    bagOutputs = getBagOutputRelations(root)
  }

  def doBagDedup() = {
    root.eliminateDuplicateBagWork(List[GHDNode](), joinAggregates)
  }

  def pushOutSelections() = {
    root.recursivelyPushOutSelections()
  }

  def getQueryPlan(): List[Rule] = {
    // do a preorder traversal of the GHDNodes to get the query plans
    val plan = root.recursivelyGetQueryPlan(joinAggregates, needTopDownPass())
    if(needTopDownPass()) {
      getTopDownPass()::plan
    } else {
      plan
    }
  }

  def needTopDownPass():Boolean = {
    // no need to do the top down pass since the root has all the materialized attrs
    return !outputRelation.attrs.values.forall(root.noChildAttrSet.contains(_))
  }

  def getTopDownPass():Rule = {
    val relationsInTopDownPass = root.getResult(true).rel::root.getDescendantNames(outputRelation.attrs)
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

