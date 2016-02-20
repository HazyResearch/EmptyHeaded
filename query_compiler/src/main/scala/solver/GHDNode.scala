package duncecap

import java.util

import duncecap.attr._
import duncecap.serialized.Attribute
import org.apache.commons.math3.optim.linear._
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType


class GHDNode(override val rels: List[OptimizerRel],
              override val selections:Array[Selection])
  extends EHNode(rels, selections) with Iterable[GHDNode] {
  var subtreeRels = rels.toSet
  var bagName: String = null
  var isDuplicateOf: Option[String] = None
  var bagFractionalWidth: Double = 0
  var bagWidth: Int = 0
  var depth: Int = 0
  var level:Int = 0
  var projectedOutAttrs: Set[Attr] = null

  /**
   * Iterator returns the nodes in the GHD in preorder traversal order
   */
  override def iterator: Iterator[GHDNode] = {
    Iterator(this) ++ children.iterator.map(_.iterator).flatten
  }

  override def toString: String = {
    return s"""GHDNode(${rels}, ${children})"""
  }

  override def size(): Int = {
    return iterator.size
  }

  /**
   * This is intended for use by GHDSolver, so we don't distinguish between trees with different vars set
   * in GHD's post-processing pass
   */
  override def equals(o: Any) = o match {
    case that: GHDNode => that.rels.toSet.equals(rels.toSet) && that.children.toSet.equals(children.toSet)
    case _ => false
  }

  def attrNameAgnosticEquals(otherNode: GHDNode, joinAggregates:Map[String, Aggregation]): Boolean = {
    if (this.subtreeRels.size != otherNode.subtreeRels.size || !(attrSet & otherNode.attrSet).isEmpty) return false
    return matchRelations(this.subtreeRels.toSet, otherNode.subtreeRels.toSet, this, otherNode, Map[Attr, Attr](), joinAggregates) ;
  }

  private def matchRelations(rels:Set[OptimizerRel],
                             otherRels:Set[OptimizerRel],
                             thisNode:GHDNode,
                             otherNode:GHDNode,
                             attrMap: Map[Attr, Attr],
                             joinAggregates:Map[String, Aggregation]): Boolean = {
    return false
    /* if (rels.isEmpty && otherRels.isEmpty) return true

    val matches = otherRels.map(otherRel => (otherRels-otherRel, PlanUtil.attrNameAgnosticRelationEquals(otherNode.outputRelation, otherRel, thisNode.outputRelation, rels.head, attrMap, joinAggregates))).filter(m => {
      m._2.isDefined
    })
    return matches.exists(m => {
      matchRelations(rels.tail, m._1, this, otherNode, m._2.get, joinAggregates)
    }) */
  }

  override def hashCode = 41 * rels.hashCode() + children.toSet.hashCode()

  def setBagName(name:String): Unit = { bagName = name }

  /**
   * Does not change execution, but for clarity/cosmetic reasons we push rels w/ selections out from each bag B
   * so that
   *
   * Returns a new copy of the tree
   */
  def recursivelyPushOutSelections(): GHDNode = {
    val (withoutSelections, withSelections) = rels.partition(
      rel => (rel.attrs.values.toSet intersect selections.map(selection => selection.getAttr()).toSet).isEmpty)
    if (!withoutSelections.isEmpty) {
      val newNode = new GHDNode(withoutSelections, selections)
      newNode.children = children.map(_.recursivelyPushOutSelections) ::: withSelections.map(rel => new GHDNode(List(rel), selections))
      return newNode
    } else {
      val newNode = new GHDNode(rels, selections)
      children.map(_.recursivelyPushOutSelections)
      return newNode
    }
  }

  def eliminateDuplicateBagWork(seen:List[GHDNode], joinAggregates:Map[String, Aggregation]): List[GHDNode] = {
    var newSeen = seen
    children.foreach(c => {
      newSeen = c.eliminateDuplicateBagWork(newSeen, joinAggregates)
    })
    val prevSeenDuplicate = newSeen.find(bag => bag.attrNameAgnosticEquals(this, joinAggregates))
    prevSeenDuplicate.map(p => {
      isDuplicateOf = Some(p.bagName)
    })
    newSeen = if (prevSeenDuplicate.isEmpty) this::newSeen else newSeen
    return newSeen
  }

  def getBagInfo(joinAggregates:Map[String, Aggregation]): QueryPlanBagInfo = {
    val jsonRelInfo = getRelationInfo()
    new QueryPlanBagInfo(
      bagName,
      isDuplicateOf,
      outputRelation.attrs.values,
      outputRelation.anno.values.head, // TODO (sctu): in the future it may not be the case that it's the first one
      jsonRelInfo,
      getNPRRInfo(joinAggregates),
      None,
      scalars.map(_.name))
  }

  def setDescendantNames(level:Int): Unit = {
    children.map(childAndIndex => {
      val attrNames = childAndIndex.attrSet.toList.sortBy(attributeOrdering.indexOf(_)).mkString("_")
      childAndIndex.setBagName(s"bag_${level}_${attrNames}")
      childAndIndex.level = level
    })
    children.map(child => {child.setDescendantNames(level + 1)})
  }


  def getRelationInfo(forTopLevelSummary:Boolean = false): List[QueryPlanRelationInfo] = {
    val relsToUse =
      if (forTopLevelSummary) {
        rels
      } else {
        subtreeRels.toList
      }
    PlanUtil.getRelationInfoBasedOnName(forTopLevelSummary, relsToUse, attributeOrdering)
  }

  def recreateFromAttrMappings: Unit = {
    attrToRels = PlanUtil.createAttrToRelsMapping(attrSet, subtreeRels.toList)
    //attrToSelection = attrSet.map(attr => (attr, PlanUtil.getSelection(attr, attrToRels))).toMap
    // TODO (sctu): fix this or delete it
    children.map(child => child.recreateFromAttrMappings)
  }

  override def setAttributeOrdering(ordering: List[Attr] ): Unit = {
    attributeOrdering = ordering
    children.map(child => child.setAttributeOrdering(ordering))
  }

  /**
   * Compute what is projected out in this bag, and what this bag's output relation is
   */
  def computeProjectedOutAttrsAndOutputRelation(annotationType:String,
                                                outputAttrs:Set[Attr],
                                                attrsFromAbove:Set[Attr]): OptimizerRel = {

    // how do I use Projected,
    projectedOutAttrs = attrSet -- (outputAttrs ++ attrsFromAbove)
    val keptAttrs = attrSet intersect (outputAttrs ++ attrsFromAbove)
    val equalitySelectedAttrs = attrSet.filter(attr => !getSelection(attr).isEmpty)
    // Right now we only allow a query to have one type of annotation, so
    // we take the annotation type from an arbitrary relation that was joined in this bag
    outputRelation = new OptimizerRel(
      bagName,
      Attributes(keptAttrs.unzip._1.toList),
      Annotations(List(annotationType)),
      false,
      keptAttrs.toSet
    )
    val childrensOutputRelations = children.map(child => {
      child.computeProjectedOutAttrsAndOutputRelation(
        annotationType,
        outputAttrs,
        attrsFromAbove ++ attrSet -- equalitySelectedAttrs)
    })
    subtreeRels ++= childrensOutputRelations
    scalars = childrensOutputRelations.filter(rel => rel.attrs.values.isEmpty)
    return outputRelation
  }

  def computeDepth : Unit = {
    if (children.isEmpty) {
      depth = 1
    } else {
      val childrenDepths = children.map(x => {
        x.computeDepth
        x.depth
      })
      depth = childrenDepths.max + 1
    }
  }

  def getNumBags(): Int = {
    1 + children.foldLeft(0)((accum : Int, child : GHDNode) => accum + child.getNumBags())
  }

  def scoreTree(): Int = {
    bagWidth = rels.size
    return children.map((child: GHDNode) => child.scoreTree()).foldLeft(bagWidth)((accum: Int, x: Int) => if (x > accum) x else accum)
  }

  private def getMatrixRow(attr : String, rels : List[OptimizerRel]): Array[Double] = {
    val presence = rels.map((rel : OptimizerRel) => if (rel.attrs.values.toSet.contains(attr)) 1.0 else 0)
    return presence.toArray
  }

  private def fractionalScoreNode(): Double = { // TODO: catch UnboundedSolutionException
  val myRealRels = rels.filter(!_.isImaginary)
    val unselectedAttrSet = attrSet -- attrToSelection.keys.filter(attr => {
      attrToSelection.get(attr).isDefined && !attrToSelection.get(attr).get.isEmpty
    }) // don't bother covering attributes that are equality selected
    val realRels = myRealRels:::children.flatMap(child => child.rels.filter(!_.isImaginary))
    if (realRels.isEmpty) {
      return 1 // just return 1 because we're going to delete this node anyways
    }
    val objective = new LinearObjectiveFunction(realRels.map((rel : OptimizerRel) => 1.0).toArray, 0)
    // constraints:
    val constraintList = new util.ArrayList[LinearConstraint]
    unselectedAttrSet.map((attr : String) => {
      constraintList.add(new LinearConstraint(getMatrixRow(attr, realRels), Relationship.GEQ,  1.0))
    })
    val constraints = new LinearConstraintSet(constraintList)
    val solver = new SimplexSolver
    val solution =
      try {
        solver.optimize(objective, constraints, GoalType.MINIMIZE, new NonNegativeConstraint(true))
      } catch {
        case e: NoFeasibleSolutionException => {
          val it = constraintList.iterator
          while (it.hasNext) {
            println(it.next().getCoefficients)
          }
          println(rels.filter(_.isImaginary))
          throw e
        }
      }
    return solution.getValue
  }

  def fractionalScoreTree() : Double = {
    bagFractionalWidth = fractionalScoreNode()
    return children.map((child: GHDNode) => child.fractionalScoreTree())
      .foldLeft(bagFractionalWidth)((accum: Double, x: Double) => if (x > accum) x else accum)
  }

  def getQueryPlan(aggMap:Map[String, Aggregation]): Rule = {
    return Rule(
      getResult(),
      None /* TODO: handle recursion */,
      getOperation(),
      getOrder(),
      getProject(),
      getJoin(),
      getAggregations(aggMap),
      getFilters())
  }

  def recursivelyGetQueryPlan(aggMap:Map[String, Aggregation]): List[Rule] = {
    getQueryPlan(aggMap)::children.flatMap(_.recursivelyGetQueryPlan(aggMap))
  }

  def getResult(): Result = {
    Result(outputRelation)
  }

  def getFilters() = {
    Filters(selections.toList)
  }

  def getOperation(): Operation = {
    Operation("*")
  }

  def getOrder(): Order = {
    Order(Attributes(attributeOrdering))
  }

  def getProject(): Project = {
    Project(Attributes(List()))
  }

  def getJoin(): Join = {
    Join(rels)
  }

  def getAggregations(aggMap:Map[String, Aggregation]) = {
    Aggregations(attrSet.map(attr => {
      val aggOption = aggMap.get(attr)
      assert(aggOption.isDefined)
      aggOption.get
    }).toList)
  }
}
