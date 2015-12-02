package DunceCap

import DunceCap.attr._

import scala.collection.mutable

class Recursion(recurse:RecursionNode,
                baseCase:RecursionNode) extends QueryPlanPostProcessor {
  override def doPostProcessingPass = {
    recurse.outputRelation = recurse.outputRel
    recurse.createAttrToRelsMapping
    recurse.setAttributeOrdering(AttrOrderingUtil.get_attribute_ordering(mutable.Set[EHNode](recurse), recurse.outputRelation))
    baseCase.outputRelation = baseCase.outputRel
    baseCase.createAttrToRelsMapping
    baseCase.setAttributeOrdering(AttrOrderingUtil.get_attribute_ordering(mutable.Set[EHNode](baseCase), baseCase.outputRelation))
  }

  override def getQueryPlan: QueryPlan = {
    new QueryPlan(
      "recursion",
      getRelationsSummary(),
      getOutputInfo(),
      getPlanBody(),
      List())
  }

  private def getRelationsSummary(): List[QueryPlanRelationInfo] = {
    (PlanUtil.getRelationInfoBasedOnName(true, recurse.join.filter(rel => rel.name != baseCase.outputRel.name), recurse.attributeOrdering):::
      PlanUtil.getRelationInfoBasedOnName(true, baseCase.join, baseCase.attributeOrdering)).distinct
  }

  private def getOutputInfo(): QueryPlanOutputInfo = {
    new QueryPlanOutputInfo(
      recurse.outputRelation.name,
      PlanUtil.getNumericalOrdering(recurse.attributeOrdering, recurse.outputRelation),
      recurse.outputRelation.annotationType)
  }

  private def getPlanBody(): List[QueryPlanBagInfo] = {
    baseCase.bagName = "base_case"
    val baseBag = baseCase.getBagInfo(baseCase.joinAggregates)
    recurse.bagName = "recursion"
    recurse.join.foreach(rel => {
      if (rel.name == baseCase.outputRelation.name) {
        rel.name = baseCase.bagName
      }
    })
    val recurseBag = recurse.getBagInfo(recurse.joinAggregates)
    List(baseBag, recurseBag)
  }
}

case class RecursionNode(val join:List[QueryRelation],
                         val joinAggregates:AggInfo,
                         val outputRel:QueryRelation,
                         val convergenceCriteria:Option[ConverganceCriteria]) extends EHNode(join) {
  var bagName: String = null

  def getBagInfo(joinAggregates:Map[String,ParsedAggregate]): QueryPlanBagInfo = {
    val jsonRelInfo =  PlanUtil.getRelationInfoBasedOnName(false, join, attributeOrdering)
    new QueryPlanBagInfo(
      bagName,
      None,
      outputRelation.attrNames,
      outputRelation.annotationType,
      jsonRelInfo,
      getNPRRInfo(joinAggregates),
      getConvergenceInfo)
  }

  private def getConvergenceInfo(): Option[QueryPlanRecursion] = {
    convergenceCriteria.map(cc => QueryPlanRecursion(cc.converganceType, cc.converganceOp, cc.converganceCondition))
  }

  override def setAttributeOrdering(ordering: List[Attr]): Unit = {
    println("set attribute ordering")
    println(ordering)
    attributeOrdering = ordering
  }
}
