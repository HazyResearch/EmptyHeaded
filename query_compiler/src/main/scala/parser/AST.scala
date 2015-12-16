package DunceCap

import DunceCap.attr.{Attr, AttrType, AnnotationType}

import scala.collection.immutable.TreeSet

abstract trait ASTStatement
abstract trait ASTConvergenceCondition

case class ASTItersCondition(iters:Int) extends ASTConvergenceCondition
case class ASTEpsilonCondition(eps:Double) extends ASTConvergenceCondition

case class InfiniteRecursionException(what:String) extends Exception(
  s"""Infinite recursion detected in eval graph: ${what}""")
case class NoOutputTypeException(lhsName:String) extends Exception(
  s"""No output type for ${lhsName}, likely due to failure to run typechecking pass""")
case class JoinTypeMismatchException(attr:Attr, attrTypes:Set[AttrType]) extends Exception(
  s"""Found multiple types [${attrTypes.mkString(",")}] for attribute ${attr}""")
case class OutputAttributeNotFoundInJoinException(attr:Attr) extends Exception(
  s"""Output attribute ${attr} not found in query body""")
case class NoTypeFoundException(relName:String, attrPos:Int) extends Exception(
  s"""No type found for ${attrPos}th attribute of relation ${relName}"""
)

case class ASTQueryStatement(lhs:QueryRelation,
                             convergence:Option[ASTConvergenceCondition],
                             joinType:String,
                             join:List[QueryRelation],
                             joinAggregates:Map[String,ParsedAggregate]) extends ASTStatement {
  val attrSet = join.foldLeft(TreeSet[String]())(
    (accum: TreeSet[String], rel: QueryRelation) => accum | TreeSet[String](rel.attrNames: _*))
  val attrToRels = PlanUtil.createAttrToRelsMapping(attrSet, join)
  private var outputType:List[AttrType] = null

  // TODO (sctu) : ignoring everything except for join, joinAggregates for now
  def dependsOn(statement: ASTQueryStatement): Boolean = {
    val namesInThisStatement = (join.map(rels => rels.name)
      :::joinAggregates.values.map(parsedAgg => parsedAgg.expressionLeft+parsedAgg.expressionRight).toList).toSet
    namesInThisStatement.find(name => name.contains(statement.lhs.name)).isDefined
  }

  /**
   * Throws error if this expression can't possible typecheck,
   * otherwise sets outputType
   */
  def typecheck(): Unit = {
    val attrToType = attrSet.map(attr => {
      val attrType  = attrToRels(attr).map(rel => Environment.getType(rel.name, rel.attrNames.indexOf(attr))).toSet
      if (attrType.size != 1) {
        throw JoinTypeMismatchException(attr, attrType)
      } else {
        (attr, attrType.head)
      }
    }).toMap
    val attrTypes = lhs.attrNames.map(attrName => attrToType.get(attrName) match {
      case Some(x) => x
      case None => throw OutputAttributeNotFoundInJoinException(attrName)
    } )
    outputType = attrTypes
  }

  /**
   *
   * @return
   */
  def getOutputType() : List[AttrType] = {
    if (outputType == null) {
      throw NoOutputTypeException(lhs.name)
    } else {
      return outputType
    }
  }

  def computePlan(config:Config, isRecursive:Boolean): QueryPlan = {
    val missingRel = join.find(rel => !Environment.setAnnotationAccordingToConfig(rel))
    if (missingRel.isDefined) {
      throw RelationNotFoundException(missingRel.get.name)
    }

    if (!config.nprrOnly) {
      val rootNodes = GHDSolver.getMinFHWDecompositions(join)
      val candidates = rootNodes.map(r => new GHD(r, join, joinAggregates, lhs))
      candidates.map(c => c.doPostProcessingPass())
      val chosen = HeuristicUtils.getGHDsWithMaxCoveringRoot(
        HeuristicUtils.getGHDsOfMinHeight(HeuristicUtils.getGHDsWithMinBags(candidates)))
      if (config.bagDedup) {
        chosen.head.doBagDedup
      }
      chosen.head.getQueryPlan
    } else {
      // since we're only using NPRR, just create a single GHD bag
      val oneBag = new GHD(new GHDNode(join) ,join, joinAggregates, lhs)
      oneBag.doPostProcessingPass
      oneBag.getQueryPlan
    }
  }
}
