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
case class OutputAttributeAggregatedAwayException(attr:Attr) extends Exception(
  s"""Output attribute ${attr} is aggregated away""")
case class NoTypeFoundException(relName:String, attrPos:Int) extends Exception(
  s"""No type found for ${attrPos}th attribute of relation ${relName}""")
case class MaterializationOfSelectedAttrUnsupportedException(attrName:Attr) extends Exception(
  s"""Cannot both equality select and materialize attribute ${attrName}""")
case class MultipleSelectionsUnsupportedException(attr:Attr) extends Exception(
  s"""Cannot support multiple selections in same attribute ${attr}""")

case class ASTQueryStatement(lhs:QueryRelation,
                             convergence:Option[ASTConvergenceCondition],
                             joinType:String,
                             var join:List[QueryRelation],
                             joinAggregates:Map[String,ParsedAggregate]) extends ASTStatement {
  val attrSet = join.foldLeft(TreeSet[String]())(
    (accum: TreeSet[String], rel: QueryRelation) => accum | TreeSet[String](rel.attrNames: _*))
  val outputAttrs = attrSet -- joinAggregates.keySet
  val attrToRels = PlanUtil.createAttrToRelsMapping(attrSet, join)
  val attrToSelection = attrSet.map(attr => (attr, PlanUtil.getSelection(attr, attrToRels))).toMap
  private var outputType:List[AttrType] = null

  // TODO (sctu) : ignoring everything except for join, joinAggregates for now
  def dependsOn(statement: ASTQueryStatement): Boolean = {
    val namesInThisStatement = (join.map(rels => rels.name)
      :::joinAggregates.values.map(parsedAgg => parsedAgg.expressionLeft+parsedAgg.expressionRight).toList).toSet
    namesInThisStatement.find(name => name.contains(statement.lhs.name)).isDefined
  }

  def propagateSelections(): Unit = {
    // TODO: remove this check that could throw MaterializationOfSelectedAttrUnsupportedException later
    // when we do support this in the codegen
    val selectedAndMaterialized = attrToSelection.filter({case (attr, selects) => !selects.isEmpty})
      .keys.find(attr => lhs.attrNames.contains(attr))
    selectedAndMaterialized.foreach(attr => throw MaterializationOfSelectedAttrUnsupportedException(attr))

    join = join.map(rel => {
      val rewrittenAttrs = rel.attrs.map(attr => {
        val selections = attrToSelection(attr._1).map(selection => {
          (attr._1, selection.operation, selection.expression)
        })
        if (selections.size > 1) {
          throw MultipleSelectionsUnsupportedException(attr._1)
        } else if (selections.size == 0) {
          (attr._1, "", "")
        } else {
          selections.head
        }
      })
      QueryRelation(rel.name, rewrittenAttrs, rel.annotationType, rel.isImaginary)
    })
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
    val attrTypes = lhs.attrNames.map(attrName => {
      if (joinAggregates.get(attrName).isDefined)
        throw OutputAttributeAggregatedAwayException(attrName)
      else
        attrToType.get(attrName) match {
          case Some(x) => x
          case None => throw OutputAttributeNotFoundInJoinException(attrName)
        }
    })
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
      val rootNodes = GHDSolver.computeAJAR_GHD(join.toSet, outputAttrs)
      val candidates = rootNodes.map(r => new GHD(r, join, joinAggregates, lhs))
      candidates.map(c => c.doPostProcessingPass())
      val chosen = HeuristicUtils.getGHDsWithMaxCoveringRoot(HeuristicUtils.getGHDsWithSelectionsPushedDown(
        HeuristicUtils.getGHDsOfMinHeight(HeuristicUtils.getGHDsWithMinBags(candidates))))
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
