package DunceCap

import DunceCap.attr._

class Recursion(recurse:RecursionNode,
                baseCase:RecursionNode) extends QueryPlanPostProcessor {
  override def doPostProcessingPass = {
    recurse.outputRelation = recurse.outputRel
    recurse.setAttributeOrdering(AttrOrderingUtil.getAttributeOrdering(recurse, recurse.join, recurse.outputRelation))
    baseCase.outputRelation = baseCase.outputRel
    baseCase.setAttributeOrdering(AttrOrderingUtil.getAttributeOrdering(baseCase, baseCase.join, baseCase.outputRelation))

    baseCase.bagName = "base_case"
    val missingRel1 = baseCase.join.find(rel => !Environment.setAnnotationAccordingToConfig(rel))
    if (missingRel1.isDefined) {
      throw RelationNotFoundException(missingRel1.get.name)
    }

    recurse.bagName = "recursion"
    recurse.join.foreach(rel => {
      if (rel.name == baseCase.outputRelation.name) {
        rel.name = baseCase.bagName
        rel.annotationType = baseCase.outputRel.annotationType
      }
    })
    val nonBaseRels = recurse.join.filter(rel => rel.name != "base_case")
    val missingRel2 = nonBaseRels.find(rel => !Environment.setAnnotationAccordingToConfig(rel))
    if (missingRel2.isDefined) {
      throw RelationNotFoundException(missingRel2.get.name)
    }
  }

  override def getQueryPlan: QueryPlan = {
    new QueryPlan(
      "recursion",
      getRelationsSummary(),
      getOutputInfo(),
      getPlanBodyAndRewriteBagNames(),
      List())
  }

  private def getRelationsSummary(): List[QueryPlanRelationInfo] = {
    (PlanUtil.getRelationInfoBasedOnName(true, recurse.join.filter(rel => rel.name != "base_case"), recurse.attributeOrdering):::
      PlanUtil.getRelationInfoBasedOnName(true, baseCase.join, baseCase.attributeOrdering)).distinct
  }

  private def getOutputInfo(): QueryPlanOutputInfo = {
    new QueryPlanOutputInfo(
      recurse.outputRelation.name,
      PlanUtil.getNumericalOrdering(recurse.attributeOrdering, recurse.outputRelation),
      recurse.outputRelation.annotationType)
  }

  private def getPlanBodyAndRewriteBagNames(): List[QueryPlanBagInfo] = {
    val baseBag = baseCase.getBagInfo(baseCase.joinAggregates)
    val recurseBag = recurse.getBagInfo(recurse.joinAggregates)
    List(baseBag, recurseBag)
  }
}

case class RecursionNode(val join:List[QueryRelation],
                         val joinAggregates:AggInfo,
                         val outputRel:QueryRelation,
                         val convergenceCriteria:Option[ASTConvergenceCondition]) extends EHNode(join) {
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
      getConvergenceInfo,
      List())
  }

  private def getConvergenceInfo(): Option[QueryPlanRecursion] = {
    convergenceCriteria.map(cc => {
      cc match {
        case ASTEpsilonCondition(x) => QueryPlanRecursion("base_case", "epsilon", x.toString)
        case ASTItersCondition(x) => QueryPlanRecursion("base_case", "iterations", x.toString)
      }
    })
  }

  override def setAttributeOrdering(ordering: List[Attr]): Unit = {
    attributeOrdering = ordering
  }
}
