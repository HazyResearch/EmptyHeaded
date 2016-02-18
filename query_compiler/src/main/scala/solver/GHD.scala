package duncecap

import duncecap.attr.Attr

import scala.collection.mutable

class GHD(val root:GHDNode,
          val queryRelations:List[OptimizerRel],
          val joinAggregates:Map[String, Aggregation],
          val outputRelation:OptimizerRel) extends QueryPlanPostProcessor {
  val attributeOrdering: List[Attr] = AttrOrderingUtil.getAttributeOrdering(root, queryRelations, outputRelation)
  var depth: Int = -1
  var numBags: Int = -1
  var bagOutputs:List[OptimizerRel] = null
  var attrToRels: Map[Attr, List[OptimizerRel]] = null
  var attrToAnnotation:Map[Attr, String] = null
  var lastMaterializedAttr:Option[Attr] = None
  var nextAggregatedAttr:Option[Attr] = None

  def getQueryPlan(): QueryPlan = {
    new QueryPlan(
      "join",
      getRelationsSummary(),
      getOutputInfo(),
      getPlanFromPostorderTraversal(root).toList,
      getTopDownPassIterators())
  }

  private def getAttrsToRelationsMap(): Map[Attr, List[OptimizerRel]] = {
    PlanUtil.createAttrToRelsMapping(attributeOrdering.toSet, bagOutputs)
  }

  private def getTopDownPassIterators(): List[TopDownPassIterator] = {
    if (outputRelation.attrs.values.find(!root.attrSet.contains(_)).isEmpty) {
      // no need to do the top down pass since the root has all the materialized attrs
      return List[TopDownPassIterator]()
    }
    attrToRels = PlanUtil.createAttrToRelsMapping(attributeOrdering.toSet, bagOutputs)
    lastMaterializedAttr = if (outputRelation.attrs.values.isEmpty) {
      None
    } else {
      Some(outputRelation.attrs.values.last)
    }
    if (lastMaterializedAttr.isDefined) {
      nextAggregatedAttr = attributeOrdering
        .dropWhile(at => at != lastMaterializedAttr.get)
        .find(at => joinAggregates.contains(at))
    }
    val aggregatedPrevNextInfo = PlanUtil.getPrevAndNextAttrNames(
      PlanUtil.getOrderedAttrsWithAccessor(attributeOrdering, attrToRels),
      ((attr:Attr) => joinAggregates.get(attr).isDefined && !outputRelation.attrs.values.contains(attr)))
    return getTopDownPassIterators(mutable.Set[Attr](), mutable.Set[GHDNode](root), aggregatedPrevNextInfo)
  }

  private def getTopDownPassIterators(seen: mutable.Set[Attr],
                                      f_in:mutable.Set[GHDNode],
                                      prevNextAgg:List[(Option[Attr], Option[Attr])]): List[TopDownPassIterator] = {
    val newFrontier = mutable.Set[GHDNode]()
    var prevNextAggLeft = prevNextAgg
    val iterators = f_in.map(node => {
      val newAttrs = node.outputRelation.attrs.values.filter(attrName => !seen.contains(attrName))
      newAttrs.map(newAttr => seen.add(newAttr))
      val prevNextAggThisBag = prevNextAggLeft.take(newAttrs.size)
      prevNextAggLeft = prevNextAgg.drop(newAttrs.size)
      val attrInfo = newAttrs.zip(prevNextAggThisBag).map({case (newAttr, prevNextAggEntry) => {
        val agg = if (joinAggregates.contains(newAttr)) {
          Some(QueryPlanAggregation(
            joinAggregates.get(newAttr).get.op,
            joinAggregates.get(newAttr).get.init,
            joinAggregates.get(newAttr).get.expression,
            prevNextAggEntry._1,
            prevNextAggEntry._2))
        } else {
          None
        }
        QueryPlanAttrInfo(
          newAttr,
          PlanUtil.getAccessor(newAttr, attrToRels, attributeOrdering),
          outputRelation.attrNames.contains(newAttr),
          List[QueryPlanSelection](),
          lastMaterializedAttr.flatMap(at => {
            if (newAttr == at) {
              nextAggregatedAttr
            } else {
              None
            }
          }),
          agg,
          None,
          None
        )
      }})
      node.children.map(child => newFrontier.add(child))
      TopDownPassIterator(node.bagName, attrInfo)
    }).toList

    if (newFrontier.isEmpty) {
      return iterators
    } else {
      return iterators ::: getTopDownPassIterators(seen, newFrontier, prevNextAggLeft)
    }
  }

  def getBagOutputRelations(node:GHDNode) : List[OptimizerRel] = {
    node.outputRelation::node.children.flatMap(child => getBagOutputRelations(child))
  }

  /**
   * Summary of all the relations in the GHD
   * @return Json for the relation summary
   */
  private def getRelationsSummary(): List[QueryPlanRelationInfo] = {
    val a = getRelationSummaryFromPreOrderTraversal(root)
    a.distinct
  }

  private def getRelationSummaryFromPreOrderTraversal(node:GHDNode): List[QueryPlanRelationInfo] = {
    node.getRelationInfo(true):::node.children.flatMap(c => {getRelationSummaryFromPreOrderTraversal(c)})
  }

  private def getOutputInfo(): QueryPlanOutputInfo = {
    new QueryPlanOutputInfo(
      outputRelation.name,
      PlanUtil.getNumericalOrdering(attributeOrdering, outputRelation),
      outputRelation.annotationType)
  }

  // TODO: (sctu) this .toVector stuff is probably unnecessary, clean this up
  private def getPlanFromPostorderTraversal(node:GHDNode): Vector[QueryPlanBagInfo] = {
    node.children.toVector.flatMap(c => getPlanFromPostorderTraversal(c)):+node.getBagInfo(joinAggregates)
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

    root.computeProjectedOutAttrsAndOutputRelation(outputRelation.annotationType,outputRelation.attrNames.toSet, Set())
    root.recreateFromAttrMappings
    bagOutputs = getBagOutputRelations(root)
  }

  def doBagDedup() = {
    root.eliminateDuplicateBagWork(List[GHDNode](), joinAggregates)
  }

  def pushOutSelections() = {
    root.recursivelyPushOutSelections()
  }
}
