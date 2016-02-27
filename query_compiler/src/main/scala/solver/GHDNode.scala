package duncecap

import java.util

import duncecap.attr._
import duncecap.serialized.Attribute
import org.apache.commons.math3.optim.linear._
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType

import scala.collection.immutable.TreeSet


class GHDNode(override val rels: List[OptimizerRel],
              override val selections:Array[Selection])
  extends EHNode(rels, selections) with Iterable[GHDNode] {
  var subtreeRels = rels.toSet
  var noChildAttrSet = attrSet
  var bagName: String = null
  var isDuplicateOf: Option[String] = None
  var bagFractionalWidth: Double = 0
  var bagWidth: Int = 0
  var depth: Int = 0
  var level:Int = 0

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

  def setBagName(name:String): Unit = {
    bagName = name
    if (outputRelation != null) {
    outputRelation = OptimizerRel(
      name,
      outputRelation.attrs,
      outputRelation.anno,
      outputRelation.isImaginary,
      outputRelation.nonSelectedAttrNames)
    }
  }

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

  def setDescendantNames(level:Int, suffix:String): Unit = {
    children.map(childAndIndex => {
      val attrNames = childAndIndex.attrSet.toList.sortBy(attributeOrdering.indexOf(_)).mkString("_")
      childAndIndex.setBagName(s"bag_${level}_${attrNames}_${suffix}")
      childAndIndex.level = level
    })
    children.map(child => {child.setDescendantNames(level + 1, suffix)})
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
  def recursivelyComputeProjectedOutAttrsAndOutputRelation(annotationType:String,
                                                           outputAttrs:Set[String],
                                                           attrsFromAbove:Set[String]): OptimizerRel = {
    val equalitySelectedAttrs:Set[String] = attrSet.filter(attr => !getSelection(attr).isEmpty)
    val childrensOutputRelations = children.map(child => {
      child.recursivelyComputeProjectedOutAttrsAndOutputRelation(
        annotationType,
        outputAttrs,
        attrsFromAbove ++ attrSet -- equalitySelectedAttrs)
    })
    subtreeRels ++= childrensOutputRelations
    attrSet = subtreeRels.foldLeft(TreeSet[String]())(
      (accum: TreeSet[String], rel: OptimizerRel) => accum | TreeSet[String](rel.attrs.values: _*))

    val keptAttrs = noChildAttrSet intersect (outputAttrs ++ attrsFromAbove)
    outputRelation = new OptimizerRel(
      bagName,
      Attributes(keptAttrs.toList),
      Annotations(List(annotationType)),
      false,
      keptAttrs -- equalitySelectedAttrs
    )
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

  def getQueryPlan(aggMap:Map[String, Aggregation], queryHasTopDownPass:Boolean): Rule = {
    return Rule(
      getResult(queryHasTopDownPass),
      None /* TODO: handle recursion */,
      getOperation(),
      getOrder(),
      getProject(aggMap),
      getJoin(),
      getAggregations(aggMap),
      getFilters())
  }

  def recursivelyGetQueryPlan(aggMap:Map[String, Aggregation], queryHasTopDownPass:Boolean): List[Rule] = {
    getQueryPlan(aggMap, queryHasTopDownPass)::children.flatMap(_.recursivelyGetQueryPlan(aggMap, queryHasTopDownPass))
  }

  def getResult(queryHasTopDownPass:Boolean): Result = {
    Result(OptimizerRel.toRel(outputRelation), if (queryHasTopDownPass) true else level != 0)
  }

  def getFilters() = {
    Filters(selections.toList.filter(selection => attrSet.contains(selection.attr)))
  }

  def getOperation(): Operation = {
    Operation("*")
  }

  def getOrder(): Order = {
    Order(Attributes(attributeOrdering.filter(attr =>  attrSet.contains(attr))))
  }

  def getProject(aggMap:Map[String, Aggregation]): Project = {
    val projectedOutAttrs = attrSet -- outputRelation.attrs.values -- aggMap.keySet
    Project(Attributes(projectedOutAttrs.toList))
  }

  def getJoin(): Join = {
    Join(subtreeRels.map(rel =>
      OptimizerRel.toRel(rel)).toList)
  }

  def getAggregations(aggMap:Map[String, Aggregation]) = {
    val aggs = attrSet.flatMap(attr => {
      aggMap.get(attr)
    }).toList
    Aggregations(aggs.map(agg =>
      Aggregation(
        agg.annotation,
        agg.datatype,
        agg.operation,
        Attributes(agg.attrs.values.filter(at => attrSet.contains(at) && aggMap.contains(at))),
        agg.init,
        agg.expression
      )
    ))
  }

  def getDescendantNames(attrs:Attributes):List[Rel] = {
    val rels:List[Rel] = children
      .filter(child => !(child.outputRelation.attrs.values.toSet intersect attrs.values.toSet).isEmpty)
      .map(_.getResult(false).rel)
    rels:::children.flatMap(_.getDescendantNames(attrs))
  }
}
