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
  val noChildAttrSet = rels.filter(!_.isImaginary).foldLeft(TreeSet[String]())(
    (accum: TreeSet[String], rel: OptimizerRel) => accum | TreeSet[String](rel.attrs.values: _*))
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

  override def size: Int = 1

  override def toString: String = {
    return s"""GHDNode(${rels}, ${children})"""
  }

  /**
   * This is intended for use by GHDSolver, so we don't distinguish between trees with different vars set
   * in GHD's post-processing pass
   */
  override def equals(o: Any) = o match {
    case that: GHDNode => that.rels.toSet.equals(rels.toSet) && that.children.toSet.equals(children.toSet)
    case _ => false
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
  def recursivelyPushOutSelections(attrsFromBagAbove:Set[String]): GHDNode = {
    val (withoutSelections, withSelections) = rels.partition(
      rel => ((rel.attrs.values.toSet union rel.anno.values.toSet) intersect selections.map(selection => selection.getAttr()).toSet).isEmpty
    )
    
    if (withoutSelections.nonEmpty) {
      // In the case where a relation has one attribute output and another attribute selected on, and no other relation
      // has this attribute, we don't want to push down the selection, so we move the relation from the withSelections set
      // to the withoutSelections set.

      // The set of attributes that will be in the new bag, with some relations pushed out.
      val newNodeAttrs = withoutSelections.flatMap(_.attrs.values).toSet
      // The attributes that should be in the bag, but aren't.
      val missingAttrs = attrsFromBagAbove -- newNodeAttrs
      // The set of relations that shouldn't be pushed out.
      val noPushRelations = missingAttrs.flatMap(attr => withSelections.find(_.attrs.values.contains(attr)))
      // Updated with and without sets.
      val newWithSelections = (withSelections.toSet -- noPushRelations).toList
      val newWithoutSelections = withoutSelections ++ noPushRelations

      val newNode = new GHDNode(newWithoutSelections, selections)
      val bagAttrs = newNode.rels.flatMap(_.attrs.values).toSet
      newNode.children = children.map(_.recursivelyPushOutSelections(bagAttrs)) ::: newWithSelections.map(rel => new GHDNode(List(rel), selections))
      return newNode
    } else {
      val newNode = new GHDNode(rels, selections)
      val bagAttrs = newNode.rels.flatMap(_.attrs.values).toSet
      newNode.children = children.map(_.recursivelyPushOutSelections(bagAttrs))
      return newNode
    }
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
  def recursivelyComputeProjectedOutAttrsAndOutputRelation(annotationType:Option[List[String]],
                                                           outputAttrs:List[String],
                                                           attrsFromAbove:Set[String]): OptimizerRel = {
    val equalitySelectedAttrs:Set[String] = attrSet.filter(attr => getSelection(attr).exists({case Selection(_, op, _, _) => op == EQUALS()}))
    val childrensOutputRelations = children.map(child => {
      child.recursivelyComputeProjectedOutAttrsAndOutputRelation(
        annotationType,
        outputAttrs,
        attrsFromAbove ++ attrSet -- equalitySelectedAttrs)
    })
    subtreeRels ++= childrensOutputRelations
    attrSet = subtreeRels.foldLeft(TreeSet[String]())(
      (accum: TreeSet[String], rel: OptimizerRel) => accum | TreeSet[String](rel.attrs.values: _*))

    val keptAttrs = noChildAttrSet intersect (outputAttrs.toSet ++ attrsFromAbove)
    outputRelation = new OptimizerRel(
      bagName,
      Attributes(keptAttrs.toList.sortBy(outputAttrs.indexOf(_))),
      if (annotationType.isEmpty) Annotations(List()) else Annotations(annotationType.get),
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
    val nodeAttrSet = rels.flatMap(_.attrs.values).toSet
    val unselectedAttrSet = nodeAttrSet -- attrToSelection.keys.filter(attr => {
            attrToSelection.get(attr).isDefined && !attrToSelection.get(attr).get.isEmpty
          }) // don't bother covering attributes that are equality selected
    // We take all the rels on this node, including the imaginary ones.
    if (rels.isEmpty) {
      return 1 // just return 1 because we're going to delete this node anyways
    }
    val objective = new LinearObjectiveFunction(rels.map((rel : OptimizerRel) => 1.0).toArray, 0)
    // constraints:
    val constraintList = new util.ArrayList[LinearConstraint]
    unselectedAttrSet.map((attr : String) => {
      constraintList.add(new LinearConstraint(getMatrixRow(attr, rels), Relationship.GEQ,  1.0))
    })
    val constraints = new LinearConstraintSet(constraintList)
    val solver = new SimplexSolver
    val solution =
      try {
        solver.optimize(objective, constraints, GoalType.MINIMIZE, new NonNegativeConstraint(true))
      } catch {
        case e: NoFeasibleSolutionException => {
          val it = constraintList.iterator
          println(unselectedAttrSet)
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

  def getQueryPlan(aggMap:Map[String, List[Aggregation]], queryHasTopDownPass:Boolean, prevRules:List[Rule]): Rule = {
    return Rule(
      getResult(queryHasTopDownPass, aggMap),
      None /* TODO: handle recursion */,
      getOperation(),
      getOrder(),
      getProject(aggMap),
      getJoin(),
      getAggregations(aggMap, prevRules),
      getFilters())
  }

  def recursivelyGetQueryPlan(aggMap:Map[String, List[Aggregation]], queryHasTopDownPass:Boolean, prevRules:List[Rule]): List[Rule] = {
    getQueryPlan(aggMap, queryHasTopDownPass, prevRules)::children.flatMap(_.recursivelyGetQueryPlan(aggMap, queryHasTopDownPass, prevRules))
  }

  def getResult(queryHasTopDownPass:Boolean, aggMap:Map[String, List[Aggregation]]): Result = {
    Result(Rel(
      outputRelation.name,
      outputRelation.attrs,
      if (getAggregations(aggMap).values.isEmpty && aggMap.nonEmpty) Annotations(List()) else outputRelation.anno),
      if (queryHasTopDownPass) true else level != 0)
  }

  def getFilters() = {
    // OR Filters don't have their attrs in the attr field in Selection, so we need to get them separately.
    val orFilters = selections.filter(selection => {
      selection.value match {
        case SelectionOrList(filtersList) => {
          val involvedAttrs = filtersList.flatMap(filters => filters.values.map(_.attr))
          involvedAttrs.toSet.intersect(attrSet).nonEmpty
        }
        case _ => false
      }
    })
    val annos = rels.flatMap(_.anno.values).toSet
    Filters(orFilters.toList ::: selections.toList.filter(selection => attrSet.union(annos).contains(selection.attr)))
  }

  def getOperation(): Operation = {
    Operation("*")
  }

  def getOrder(): Order = {
    Order(Attributes(attributeOrdering.filter(attr =>  attrSet.contains(attr))))
  }

  def getProject(aggMap:Map[String, List[Aggregation]]): Project = {
    val projectedOutAttrs = attrSet --
      outputRelation.attrs.values --
      getAggregations(aggMap).values.flatMap(agg => agg.attrs.values)
    Project(Attributes(projectedOutAttrs.toList))
  }

  def getJoin(): Join = {
    Join(subtreeRels.map(rel =>
      OptimizerRel.toRel(rel)).toList.distinct)
  }

  def computePrevRulesDependedOn(expression:String,
                                 prevRules:List[Rule]): List[Rel] = {
    val dependedOnRules = prevRules.filter(rule => expression.indexOf(rule.result.rel.name) != -1)
    // you should only ever depend on scalars
    assert(dependedOnRules.forall(rule => rule.result.rel.attrs.values.isEmpty))
    return dependedOnRules.map(rule => rule.result.rel)
  }

  def getAggregations(aggMap:Map[String, List[Aggregation]], prevRules:List[Rule] = List()) = {
    // If the attribute is being processed in this bag, isn't materialized,
    // and is in aggMap
    val aggs = (attrSet -- outputRelation.attrs.values).flatMap(attr => {
      aggMap.getOrElse(attr, List())
    }).toList

    Aggregations(aggs.flatMap(agg => {
      val newAgg = Aggregation(
        agg.annotation,
        agg.datatype,
        agg.operation,
        Attributes(agg.attrs.values
          .filter(
            at =>
              (attrSet -- outputRelation.attrs.values).contains(at)
                && aggMap.contains(at)
                /*&& !selections.exists(select => select.attr == at)*/)),
        agg.init,
        agg.expression,
        computePrevRulesDependedOn(agg.init + " " + agg.expression, prevRules),
        agg.innerExpression
      )
      if (newAgg.attrs.values.isEmpty) {
        None
      } else {
        Some(newAgg)
      }
    }))
  }

  def getDescendants(attrs:Attributes, alreadyProvidedByHigherBag:Set[String], aggMap:Map[String, List[Aggregation]]):List[Rel] = {
    children.flatMap(child => {
      val hasMaterializedAttr = !(child.outputRelation.attrs.values.toSet intersect attrs.values.toSet).isEmpty
      val hasUnseenAttr = !(child.outputRelation.attrs.values.toSet subsetOf alreadyProvidedByHigherBag)

      if (hasMaterializedAttr && hasUnseenAttr) {
        child.getResult(false, aggMap).rel::child.getDescendants(attrs, alreadyProvidedByHigherBag ++ child.outputRelation.attrs.values.toSet,aggMap)
      } else {
        child.getDescendants(attrs, alreadyProvidedByHigherBag, aggMap)
      }
    })
  }
}
