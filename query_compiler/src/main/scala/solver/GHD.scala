package DunceCap

import java.util

import DunceCap.attr.{AttrInfo, Attr}
import org.apache.commons.math3.optim.linear._
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType

import scala.collection.immutable.TreeSet
import scala.collection.mutable


class GHD(val root:GHDNode,
          val queryRelations: List[QueryRelation],
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

  /**
   *"output":{
		"name":"TriangleCount",
		"ordering":[],
		"annotation":"long"
	  },
   */
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
    root.createAttrToRelsMapping
    bagOutputs = getBagOutputRelations(root)
  }

  def doBagDedup() = {
    root.eliminateDuplicateBagWork(List[GHDNode](), joinAggregates)
  }
}

abstract class EHNode(val rels: List[QueryRelation]) {
  val attrSet = rels.foldLeft(TreeSet[String]())(
    (accum: TreeSet[String], rel: QueryRelation) => accum | TreeSet[String](rel.attrNames: _*))

  var attrToRels:Map[Attr,List[QueryRelation]] = null
  var outputRelation: QueryRelation = null
  var attributeOrdering: List[Attr] = null
  var children: List[GHDNode] = List()

  def createAttrToRelsMapping: Unit = {
    attrToRels = PlanUtil.createAttrToRelsMapping(attrSet, rels)
  }
  def setAttributeOrdering(ordering: List[Attr] )

  private def getSelection(attr:Attr): List[QueryPlanSelection] = {
    PlanUtil.getSelection(attr, attrToRels)
  }

  def getAccessor(attr:Attr): List[QueryPlanAccessor] = {
    PlanUtil.getAccessor(attr, attrToRels, attributeOrdering)
  }

  def getOrderedAttrsWithAccessor(): List[Attr] = {
    PlanUtil.getOrderedAttrsWithAccessor(attributeOrdering, attrToRels)
  }

  private def getNextAnnotatedForLastMaterialized(attr:Attr, joinAggregates:Map[String,ParsedAggregate]): Option[Attr] = {
    if (!outputRelation.attrNames.isEmpty && outputRelation.attrNames.last == attr) {
      attributeOrdering.dropWhile(a => a != attr).tail.find(a => joinAggregates.contains(a) && attrSet.contains(a))
    } else {
      None
    }
  }

  protected def getNPRRInfo(joinAggregates:Map[String,ParsedAggregate]) = {
    val attrsWithAccessor = getOrderedAttrsWithAccessor()
    val prevAndNextAttrMaterialized = PlanUtil.getPrevAndNextAttrNames(
      attrsWithAccessor,
      ((attr:Attr) => outputRelation.attrNames.contains(attr)))
    val prevAndNextAttrAggregated = PlanUtil.getPrevAndNextAttrNames(
      attrsWithAccessor,
      ((attr:Attr) => joinAggregates.get(attr).isDefined && !outputRelation.attrNames.contains(attr)))

    attrsWithAccessor.zip(prevAndNextAttrMaterialized.zip(prevAndNextAttrAggregated)).flatMap(attrAndPrevNextInfo => {
      val (attr, prevNextInfo) = attrAndPrevNextInfo
      val (materializedInfo, aggregatedInfo) = prevNextInfo
      val accessor = getAccessor(attr)
      if (accessor.isEmpty) {
        None // this should not happen
      } else {
        Some(new QueryPlanAttrInfo(
          attr,
          accessor,
          outputRelation.attrNames.contains(attr),
          getSelection(attr),
          getNextAnnotatedForLastMaterialized(attr, joinAggregates),
          PlanUtil.getAggregation(joinAggregates, attr, aggregatedInfo),
          materializedInfo._1,
          materializedInfo._2))
      }
    })
  }
}

class GHDNode(override val rels: List[QueryRelation]) extends EHNode(rels) with Iterable[GHDNode] {
  var subtreeRels = rels
  var bagName: String = null
  var isDuplicateOf: Option[String] = None
  var bagFractionalWidth: Double = 0
  var bagWidth: Int = 0
  var depth: Int = 0
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

  /**
   * This is intended for use by GHDSolver, so we don't distinguish between trees with different vars set
   * in GHD's post-processing pass
   */
  override def equals(o: Any) = o match {
    case that: GHDNode => that.rels.equals(rels) && that.children.toSet.equals(children.toSet)
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

  def setDescendantNames(depth:Int): Unit = {
    children.map(childAndIndex => {
      val attrNames = childAndIndex.attrSet.toList.sortBy(attributeOrdering.indexOf(_)).mkString("_")
      childAndIndex.setBagName(s"bag_${depth}_${attrNames}")
    })
    children.map(child => {child.setDescendantNames(depth+1)})
  }


  def getRelationInfo(forTopLevelSummary:Boolean = false): List[QueryPlanRelationInfo] = {
    val relsToUse =
      if (forTopLevelSummary) {
        rels
      } else {
        subtreeRels
      }
    
    PlanUtil.getRelationInfoBasedOnName(forTopLevelSummary, relsToUse, attributeOrdering)
  }



  override def createAttrToRelsMapping: Unit = {
    attrToRels = PlanUtil.createAttrToRelsMapping(attrSet, subtreeRels)
    children.map(child => child.createAttrToRelsMapping)
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
    // Right now we only allow a query to have one type of annotation, so
    // we take the annotation type from an arbitrary relation that was joined in this bag
    outputRelation = new QueryRelation(bagName, keptAttrs.map(attr =>(attr, "", "")).toList, annotationType)
    val childrensOutputRelations = children.map(child => {
      child.computeProjectedOutAttrsAndOutputRelation(annotationType,outputAttrs, attrsFromAbove ++ attrSet)
    })
    subtreeRels ++= childrensOutputRelations
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
    bagWidth = attrSet.size
    return children.map((child: GHDNode) => child.scoreTree()).foldLeft(bagWidth)((accum: Int, x: Int) => if (x > accum) x else accum)
  }

  private def getMatrixRow(attr : String, rels : List[QueryRelation]): Array[Double] = {
    val presence = rels.map((rel : QueryRelation) => if (rel.attrNames.toSet.contains(attr)) 1.0 else 0)
    return presence.toArray
  }

  private def fractionalScoreNode(): Double = { // TODO: catch UnboundedSolutionException
  val objective = new LinearObjectiveFunction(rels.map((rel : QueryRelation) => 1.0).toArray, 0)
    // constraints:
    val constraintList = new util.ArrayList[LinearConstraint]
    attrSet.map((attr : String) => constraintList.add(new LinearConstraint(getMatrixRow(attr, rels), Relationship.GEQ,  1.0)))
    val constraints = new LinearConstraintSet(constraintList)
    val solver = new SimplexSolver
    val solution = solver.optimize(objective, constraints, GoalType.MINIMIZE, new NonNegativeConstraint(true))
    return solution.getValue
  }

  def fractionalScoreTree() : Double = {
    bagFractionalWidth = fractionalScoreNode()
    return children.map((child: GHDNode) => child.fractionalScoreTree())
      .foldLeft(bagFractionalWidth)((accum: Double, x: Double) => if (x > accum) x else accum)
  }
}
