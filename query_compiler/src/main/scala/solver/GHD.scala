package DunceCap

import java.util

import DunceCap.attr.{AttrInfo, Attr}
import org.apache.commons.math3.optim.linear._
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType

import scala.collection.immutable.TreeSet
import scala.collection.mutable


object GHD {
  def getNumericalOrdering(attributeOrdering:List[Attr], rel:QueryRelation): List[Int] = {
    attributeOrdering.map(a => rel.attrNames.indexOf(a)).filter(pos => {
      pos != -1
    })
  }

  def getOrderedAttrsWithAccessor(attributeOrdering:List[Attr], attrToRels:Map[Attr, List[QueryRelation]]): List[Attr] = {
    attributeOrdering.flatMap(attr => {
      val accessor = getAccessor(attr, attrToRels)
      if (accessor.isEmpty) {
        None
      } else {
        Some(attr)
      }
    })
  }

  def getAccessor(attr:Attr, attrToRels:Map[Attr, List[QueryRelation]]): List[QueryPlanAccessor] = {
    attrToRels.get(attr).getOrElse(List()).map(rel => {
      new QueryPlanAccessor(rel.name, rel.attrNames,(rel.attrNames.last == attr && rel.annotationType != "void*"))
    })
  }

  def getAggregation(joinAggregates:Map[String,ParsedAggregate],
                             attr:Attr,
                             prevNextInfo:(Option[Attr], Option[Attr])): Option[QueryPlanAggregation] = {
    joinAggregates.get(attr).map(parsedAggregate => {
      new QueryPlanAggregation(parsedAggregate.op, parsedAggregate.init, parsedAggregate.expression, prevNextInfo._1, prevNextInfo._2)
    })
  }

  def getSelection(attr:Attr, attrToRels:Map[Attr, List[QueryRelation]]): List[QueryPlanSelection] = {
    attrToRels.get(attr).getOrElse(List())
      .flatMap(rel => rel.attrs.filter(attrInfo => attrInfo._1 == attr &&  !attrInfo._2.isEmpty))
      .map(attrInfo => QueryPlanSelection(attrInfo._2, attrInfo._3))
  }

  def createAttrToRelsMapping(attrs:Set[Attr], rels:List[QueryRelation]): Map[Attr, List[QueryRelation]] = {
    attrs.map(attr =>{
      val relevantRels = rels.filter(rel => {
        rel.attrNames.contains(attr)
      })
      (attr, relevantRels)
    }).toMap
  }

  def getPrevAndNextAttrNames(attrsWithAccessor: List[Attr],
                              filterFn:(Attr => Boolean)): List[(Option[Attr], Option[Attr])] = {
    val prevAttrsMaterialized = attrsWithAccessor.foldLeft((List[Option[Attr]](), Option.empty[Attr]))((acc, attr) => {
      val prevAttrMaterialized = (
        if (filterFn(attr)) {
          Some(attr)
        } else {
          acc._2
        })
      (acc._2::acc._1, prevAttrMaterialized)
    })._1.reverse
    val nextAttrsMaterialized = attrsWithAccessor.foldRight((List[Option[Attr]](), Option.empty[Attr]))((attr, acc) => {
      val nextAttrMaterialized = (
        if (filterFn(attr)) {
          Some(attr)
        } else {
          acc._2
        })
      (acc._2::acc._1, nextAttrMaterialized)
    })._1
    prevAttrsMaterialized.zip(nextAttrsMaterialized)
  }

  /**
   * Try to match rel1 and rel2; in order for this to work, the attribute mappings such a map would imply must not contradict the
   * mappings we already know about. If this does work, return the new attribute mapping (which potentially has some new entries),
   * otherwise return None
   */
  def attrNameAgnosticRelationEquals(output1:QueryRelation,
                                     rel1: QueryRelation,
                                     output2:QueryRelation,
                                     rel2: QueryRelation,
                                     attrMap: Map[Attr, Attr],
                                     joinAggregates:Map[String, ParsedAggregate]): Option[Map[Attr, Attr]] = {
    if (!rel1.name.equals(rel2.name)) {
      return None
    }
    val zippedAttrs = (
      rel1.attrs,
      rel1.attrNames.map(attr => attrMap.get(attr)),
      rel2.attrs).zipped.toList
    if (zippedAttrs
      .exists({case (_, mappedToAttr, rel2Attr) => mappedToAttr.isDefined &&
      (!mappedToAttr.get.equals(rel2Attr._1))})) {
      // the the attribute mapping implied here violates existing mappings
      return None
    } else if (zippedAttrs.
      exists({case (rel1Attr, mappedToAttr, rel2Attr) => mappedToAttr.isEmpty && attrMap.values.exists(_.equals(rel2Attr._1))})) {
      // if the attribute mapping implied here maps an attr to another that has already been mapped to
      return None
    } else {
      zippedAttrs.foldLeft(Some(attrMap):Option[Map[Attr, Attr]])((m, attrsTriple) => {
        if (m.isEmpty) {
          None
        } else if (attrsTriple._1._2 != attrsTriple._3._2 || attrsTriple._1._3 != attrsTriple._3._3) {
          // not a match if the selections aren't the same
          None
        } else if (attrsTriple._2.isDefined) {
          // don't need to check the aggregations again because they must have been checked when we put them in attrMap
          m
        } else {
          if (output1.attrNames.contains(attrsTriple._1._1)!=output2.attrNames.contains(attrsTriple._3._1)
            || !joinAggregates.get(attrsTriple._1._1).equals(joinAggregates.get(attrsTriple._3._1))) {
            None
          } else {
            Some(m.get + (attrsTriple._1._1 -> attrsTriple._3._1))
          }
        }
      })
    }
  }
}

class GHD(val root:GHDNode,
          val queryRelations: List[QueryRelation],
          val joinAggregates:Map[String,ParsedAggregate],
          val outputRelation: QueryRelation) {
  val attributeOrdering: List[Attr] = GHDSolver.getAttributeOrdering(root, queryRelations, outputRelation)
  var depth: Int = -1
  var numBags: Int = -1
  var bagOutputs:List[QueryRelation] = null
  var attrToRels: Map[Attr, List[QueryRelation]] = null
  var attrToAnnotation:Map[Attr, String] = null
  var lastMaterializedAttr:Option[Attr] = None
  var nextAggregatedAttr:Option[Attr] = None

  def getQueryPlan(): QueryPlan = {
    val a = getRelationsSummary()
    val b = getOutputInfo()
    val c = getPlanFromPostorderTraversal(root).toList
    val d = getTopDownPassIterators()
    new QueryPlan(
      "join",
      a,
      b,
      c,
      d)
  }

  private def getAttrsToRelationsMap(): Map[Attr, List[QueryRelation]] = {
    GHD.createAttrToRelsMapping(attributeOrdering.toSet, bagOutputs)
  }

  private def getTopDownPassIterators(): List[TopDownPassIterator] = {
    if (outputRelation.attrNames.find(!root.attrSet.contains(_)).isEmpty) {
      // no need to do the top down pass since the root has all the materialized attrs
      return List[TopDownPassIterator]()
    }
    attrToRels = GHD.createAttrToRelsMapping(attributeOrdering.toSet, bagOutputs)
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
    val aggregatedPrevNextInfo = GHD.getPrevAndNextAttrNames(
      GHD.getOrderedAttrsWithAccessor(attributeOrdering, attrToRels),
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
          GHD.getAccessor(newAttr, attrToRels),
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
      GHD.getNumericalOrdering(attributeOrdering, outputRelation),
      outputRelation.annotationType)
  }

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


class GHDNode(var rels: List[QueryRelation]) {
  val attrSet = rels.foldLeft(TreeSet[String]())(
    (accum: TreeSet[String], rel: QueryRelation) => accum | TreeSet[String](rel.attrNames: _*))
  var subtreeRels = rels
  var bagName: String = null
  var isDuplicateOf: Option[String] = None
  var attrToRels:Map[Attr,List[QueryRelation]] = null
  var attributeOrdering: List[Attr] = null
  var children: List[GHDNode] = List()
  var bagFractionalWidth: Double = 0
  var bagWidth: Int = 0
  var depth: Int = 0
  var projectedOutAttrs: Set[Attr] = null
  var outputRelation: QueryRelation = null

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

    val matches = otherRels.map(otherRel => (otherRels-otherRel, GHD.attrNameAgnosticRelationEquals(otherNode.outputRelation, otherRel, thisNode.outputRelation, rels.head, attrMap, joinAggregates))).filter(m => {
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
      getNPRRInfo(joinAggregates))
  }

  def setDescendantNames(depth:Int): Unit = {
    children.map(childAndIndex => {
      val attrNames = childAndIndex.attrSet.toList.sortBy(attributeOrdering.indexOf(_)).mkString("_")
      childAndIndex.setBagName(s"bag_${depth}_${attrNames}")
    })
    children.map(child => {child.setDescendantNames(depth+1)})
  }

  private def getNPRRInfo(joinAggregates:Map[String,ParsedAggregate]) : List[QueryPlanAttrInfo] = {
    val attrsWithAccessor = getOrderedAttrsWithAccessor()
    val prevAndNextAttrMaterialized = GHD.getPrevAndNextAttrNames(
      attrsWithAccessor,
      ((attr:Attr) => outputRelation.attrNames.contains(attr)))
    val prevAndNextAttrAggregated = GHD.getPrevAndNextAttrNames(
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
          GHD.getAggregation(joinAggregates, attr, aggregatedInfo),
          materializedInfo._1,
          materializedInfo._2))
      }
    })
  }

  private def getNextAnnotatedForLastMaterialized(attr:Attr, joinAggregates:Map[String,ParsedAggregate]): Option[Attr] = {
    if (!outputRelation.attrNames.isEmpty && outputRelation.attrNames.last == attr) {
      attributeOrdering.dropWhile(a => a != attr).tail.find(a => joinAggregates.contains(a) && attrSet.contains(a))
    } else {
      None
    }
  }

  /**
   * @param attr, an attribute that definitely exists in this bag
   * @return Boolean for whether this attribute has a selection on it or not
   */
  private def getSelection(attr:Attr): List[QueryPlanSelection] = {
    GHD.getSelection(attr, attrToRels)
  }

  def getOrderedAttrsWithAccessor(): List[Attr] = {
    GHD.getOrderedAttrsWithAccessor(attributeOrdering, attrToRels)
  }



  def getAccessor(attr:Attr): List[QueryPlanAccessor] = {
    GHD.getAccessor(attr, attrToRels)
  }

  /**
   * Generates the following:
   *
  "relations": [
        {
          "name":"R",
          "ordering":[0,1],
          "attributes":[["a","b"],["b","c"],["a","c"]], # this row is optional
          "annotation":"void*"
        }
      ],
   */
  def getRelationInfo(forTopLevelSummary:Boolean = false): List[QueryPlanRelationInfo] = {
    val relsToUse =
      if (forTopLevelSummary) {
        rels.map(r => subtreeRels.find(_.name == r.name).get)  //SUSAN FIXME
      } else {
        subtreeRels
      }

    val distinctRelationNames = relsToUse.map(r => r.name).distinct
    val retValue = distinctRelationNames.flatMap(n => {
      val relationsWithName = relsToUse.filter(r => {r.name == n})
      val orderingsAndRels: List[(List[Int], List[QueryRelation])] = relationsWithName.map(rn => {
        (GHD.getNumericalOrdering(attributeOrdering, rn), rn)
      }).groupBy(p => p._1).toList.map(elem => {
        (elem._1, elem._2.unzip._2)
      })
      val or = orderingsAndRels.map(orderingAndRels => {
        val ordering = orderingAndRels._1
        val rels = orderingAndRels._2
        if (forTopLevelSummary) {
          new QueryPlanRelationInfo(rels.head.name, ordering, None, rels.head.annotationType)
        } else {
          new QueryPlanRelationInfo(rels.head.name, ordering, Some(rels.map(rel => rel.attrNames)), rels.head.annotationType)
        }
      })
      or
    })
    retValue
  }

  def createAttrToRelsMapping: Unit = {
    attrToRels = GHD.createAttrToRelsMapping(attrSet, subtreeRels)
    children.map(child => child.createAttrToRelsMapping)
  }

  def setAttributeOrdering(ordering: List[Attr] ): Unit = {
    attributeOrdering = ordering
    rels = rels.map(rel => {
      new QueryRelation(rel.name, rel.attrs.sortWith((attrInfo1, attrInfo2) => {
        ordering.indexOf(attrInfo1._1) < ordering.indexOf(attrInfo2._1)
      }), rel.annotationType)
    })
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
