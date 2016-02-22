package duncecap

import duncecap.attr.Attr

class GHD(val root:GHDNode,
          val queryRelations:List[OptimizerRel],
          val joinAggregates:Map[String, Aggregation],
          val outputRelation:Rel) extends QueryPlanPostProcessor {
  val attributeOrdering: List[Attr] = AttrOrderingUtil.getAttributeOrdering(root, queryRelations, outputRelation)
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
    root.setBagName("bag_0_"+attrNames)
    root.setDescendantNames(1)

    root.computeProjectedOutAttrsAndOutputRelation(outputRelation.anno.values.head,outputRelation.attrs.values.toSet, Set())
  //  root.recreateFromAttrMappings
    bagOutputs = getBagOutputRelations(root)
  }

  def doBagDedup() = {
    root.eliminateDuplicateBagWork(List[GHDNode](), joinAggregates)
  }

  def pushOutSelections() = {
    root.recursivelyPushOutSelections()
  }

  def getQueryPlan(): IR = {
    // do a preorder traversal of the GHDNodes to get the queryplans
    IR(root.recursivelyGetQueryPlan(joinAggregates))
  }
}

