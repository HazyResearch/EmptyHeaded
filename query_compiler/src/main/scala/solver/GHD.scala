package DunceCap

import java.util

import DunceCap.attr.Attr
import org.apache.commons.math3.optim.linear._
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType

import scala.collection.mutable

class GHD(val root:GHDNode,
          val queryRelations:List[QueryRelation],
          val joinAggregates:Map[String,ParsedAggregate],
          val outputRelation:QueryRelation) extends QueryPlanPostProcessor {
  val attributeOrdering: List[Attr] = AttrOrderingUtil.getAttributeOrdering(root, queryRelations, outputRelation)
  var depth: Int = -1
  var numBags: Int = -1
  var bagOutputs:List[QueryRelation] = null
  var attrToRels: Map[Attr, List[QueryRelation]] = null
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

  private def getAttrsToRelationsMap(): Map[Attr, List[QueryRelation]] = {
    PlanUtil.createAttrToRelsMapping(attributeOrdering.toSet, bagOutputs)
  }

  private def getTopDownPassIterators(): List[TopDownPassIterator] = {
    if (outputRelation.attrNames.find(!root.attrSet.contains(_)).isEmpty) {
      // no need to do the top down pass since the root has all the materialized attrs
      return List[TopDownPassIterator]()
    }
    attrToRels = PlanUtil.createAttrToRelsMapping(attributeOrdering.toSet, bagOutputs)
    lastMaterializedAttr = if (outputRelation.attrNames.isEmpty) {
      None
    } else {
      Some(outputRelation.attrNames.last)
    }
    if (lastMaterializedAttr.isDefined) {
      nextAggregatedAttr = attributeOrdering
        .dropWhile(at => at != lastMaterializedAttr.get)
        .find(at => joinAggregates.contains(at))
    }
    val aggregatedPrevNextInfo = PlanUtil.getPrevAndNextAttrNames(
      PlanUtil.getOrderedAttrsWithAccessor(attributeOrdering, attrToRels),
      ((attr:Attr) => joinAggregates.get(attr).isDefined && !outputRelation.attrNames.contains(attr)))
    return getTopDownPassIterators(mutable.Set[Attr](), mutable.Set[GHDNode](root), aggregatedPrevNextInfo)
  }

  private def getTopDownPassIterators(seen: mutable.Set[Attr],
                                      f_in:mutable.Set[GHDNode],
                                      prevNextAgg:List[(Option[Attr], Option[Attr])]): List[TopDownPassIterator] = {
    val newFrontier = mutable.Set[GHDNode]()
    var prevNextAggLeft = prevNextAgg
    val iterators = f_in.map(node => {
      val newAttrs = node.outputRelation.attrNames.filter(attrName => !seen.contains(attrName))
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

  def getBagOutputRelations(node:GHDNode) : List[QueryRelation] = {
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


class GHDNode(override val rels: List[QueryRelation]) extends EHNode(rels) with Iterable[GHDNode] {
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

  def attrNameAgnosticEquals(otherNode: GHDNode, joinAggregates:Map[String, ParsedAggregate]): Boolean = {
    if (this.subtreeRels.size != otherNode.subtreeRels.size || !(attrSet & otherNode.attrSet).isEmpty) return false
    return matchRelations(this.subtreeRels.toSet, otherNode.subtreeRels.toSet, this, otherNode, Map[Attr, Attr](), joinAggregates) ;
  }

  private def matchRelations(rels:Set[QueryRelation],
                     otherRels:Set[QueryRelation],
                     thisNode:GHDNode,
                     otherNode:GHDNode,
                     attrMap: Map[Attr, Attr],
                     joinAggregates:Map[String, ParsedAggregate]): Boolean = {
    if (rels.isEmpty && otherRels.isEmpty) return true

    val matches = otherRels.map(otherRel => (otherRels-otherRel, PlanUtil.attrNameAgnosticRelationEquals(otherNode.outputRelation, otherRel, thisNode.outputRelation, rels.head, attrMap, joinAggregates))).filter(m => {
      m._2.isDefined
    })
    return matches.exists(m => {
      matchRelations(rels.tail, m._1, this, otherNode, m._2.get, joinAggregates)
    })
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
    val (withoutSelects, withSelects) = rels.partition(rel => rel.nonSelectedAttrNames.size == rel.attrNames.size)
    val newNode = new GHDNode(withoutSelects)
    newNode.children = children.map(_.recursivelyPushOutSelections):::withSelects.map(rel => new GHDNode(List(rel)))
    return newNode
  }

  def eliminateDuplicateBagWork(seen:List[GHDNode], joinAggregates:Map[String, ParsedAggregate]): List[GHDNode] = {
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

  def getBagInfo(joinAggregates:Map[String,ParsedAggregate]): QueryPlanBagInfo = {
    val jsonRelInfo = getRelationInfo()
    new QueryPlanBagInfo(
      bagName,
      isDuplicateOf,
      outputRelation.attrNames,
      outputRelation.annotationType,
      jsonRelInfo,
      getNPRRInfo(joinAggregates),
      None) //SUSAN FIXME
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
    attrToSelection = attrSet.map(attr => (attr, PlanUtil.getSelection(attr, attrToRels))).toMap
    children.map(child => child.recreateFromAttrMappings)
  }

  override def setAttributeOrdering(ordering: List[Attr] ): Unit = {
    attributeOrdering = ordering
    children.map(child => child.setAttributeOrdering(ordering))
  }

  /**
   * Compute what is projected out in this bag, and what this bag's output relation is
   */
  def computeProjectedOutAttrsAndOutputRelation(annotationType:String,outputAttrs:Set[Attr], attrsFromAbove:Set[Attr]): QueryRelation = {
    projectedOutAttrs = attrSet -- (outputAttrs ++ attrsFromAbove)
    val keptAttrs = attrSet intersect (outputAttrs ++ attrsFromAbove)
    val equalitySelectedAttrs = attrSet.filter(attr => !getSelection(attr).isEmpty)
    // Right now we only allow a query to have one type of annotation, so
    // we take the annotation type from an arbitrary relation that was joined in this bag
    outputRelation = new QueryRelation(bagName, keptAttrs.map(attr =>(attr, "", "")).toList, annotationType)
    val childrensOutputRelations = children.map(child => {
      child.computeProjectedOutAttrsAndOutputRelation(
        annotationType,
        outputAttrs,
        attrsFromAbove ++ attrSet -- equalitySelectedAttrs)
    })
    subtreeRels ++= childrensOutputRelations
    scalars = childrensOutputRelations.filter(rel => rel.attrs.isEmpty)
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

  private def getMatrixRow(attr : String, rels : List[QueryRelation]): Array[Double] = {
    val presence = rels.map((rel : QueryRelation) => if (rel.attrNames.toSet.contains(attr)) 1.0 else 0)
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
    val objective = new LinearObjectiveFunction(realRels.map((rel : QueryRelation) => 1.0).toArray, 0)
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
}
