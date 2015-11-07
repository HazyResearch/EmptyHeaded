package DunceCap

import java.util

import DunceCap.attr.Attr
import org.apache.commons.math3.optim.linear._
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType

import scala.collection.immutable.TreeSet


object GHD {
  def getNumericalOrdering(attributeOrdering:List[Attr], rel:QueryRelation): List[Int] = {
    attributeOrdering.map(a => rel.attrNames.indexOf(a)).filter(pos => {
      pos != -1
    })
  }
}

class GHD(val root:GHDNode,
          val queryRelations: List[QueryRelation],
          val joinAggregates:Map[String,ParsedAggregate],
          val outputRelation: QueryRelation) {
  val attributeOrdering: List[Attr] = GHDSolver.getAttributeOrdering(root, queryRelations)
  var depth: Int = -1
  var numBags: Int = -1

  def getQueryPlan(): QueryPlan = {
    new QueryPlan("join", getRelationsSummary(), getOutputInfo(), getPlanFromPreorderTraversal(root))
  }

  /**
   * Summary of all the relations in the GHD
   * @return Json for the relation summary
   */
  private def getRelationsSummary(): List[QueryPlanRelationInfo] = {
    getRelationSummaryFromPreOrderTraversal(root).distinct
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

  private def getPlanFromPreorderTraversal(node:GHDNode): List[QueryPlanBagInfo] = {
    node.getBagInfo(joinAggregates)::node.children.flatMap(c => getPlanFromPreorderTraversal(c))
  }

  /**
   * Do a post-processiing pass to fill out some of the other vars in this class
   * You should call this before calling getQueryPlan
   */
  def doPostProcessingPass() = {
    root.computeDepth
    depth = root.depth
    numBags = root.getNumBags()
    root.setBagName(outputRelation.name)
    root.setDescendantNames(1)
    root.setAttributeOrdering(attributeOrdering)
    root.computeProjectedOutAttrsAndOutputRelation(outputRelation.attrNames.toSet, Set())
    root.createAttrToRelsMapping
  }
}


class GHDNode(var rels: List[QueryRelation]) {
  val attrSet = rels.foldLeft(TreeSet[String]())(
    (accum: TreeSet[String], rel: QueryRelation) => accum | TreeSet[String](rel.attrNames: _*))
  var subtreeRels = rels
  var bagName: String = null
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
    case that: GHDNode => that.rels.equals(rels) && that.children.equals(children)
    case _ => false
  }

  def attrNameAgnosticEquals(otherNode: GHDNode): Boolean = {
    return matchRelations(this.rels, otherNode.rels, this, otherNode) ;
  }

  def attrNameAgnosticRelationEquals(rel1: QueryRelation,
                                     rel2: QueryRelation,
                                     attrMap: Map[Attr, Attr]): (Boolean, Map[Attr, Attr]) = {
    val possibleMatch = rel1.name.equals(rel2.name) && rel1.attrNames.map(
      attrName => attrMap(attrName)).zip(rel2.attrNames).filter((maybeAttrName, attrName) => maybeAttrName.isDefined && !maybeAttrName.get.equals(attrName)).isEmpty
    return 
  }

  def matchRelations(rels:List[QueryRelation],
                     otherRels:List[QueryRelation],
                     thisNode:GHDNode,
                     otherNode:GHDNode): Boolean = {
    val matchableFromOtherRels = otherRels.filter(otherRel => attrNameAgnosticEquals(otherRel, rels.head))
  }

  override def hashCode = 41 * rels.hashCode() + children.hashCode()

  def setBagName(name:String): Unit = {
    bagName = name
  }

  def eliminateDuplicateBagWork(seen:List[GHDNode]): Unit = {

  }

  def getBagInfo(joinAggregates:Map[String,ParsedAggregate]): QueryPlanBagInfo = {
    val jsonRelInfo = getRelationInfo()
    new QueryPlanBagInfo(
      bagName,
      outputRelation.attrNames,
      outputRelation.annotationType,
      jsonRelInfo,
      getNPRRInfo(joinAggregates))
  }

  def setDescendantNames(depth:Int): Unit = {
    children.zipWithIndex.map(childAndIndex => {
      childAndIndex._1.setBagName(s"bag_${depth}_${childAndIndex._2}")
    })
    children.map(child => {child.setDescendantNames(depth+1)})
  }

  private def getNPRRInfo(joinAggregates:Map[String,ParsedAggregate]) : List[QueryPlanNPRRInfo] = {
    val attrsWithAccessor = getOrderedAttrsWithAccessor()
    val prevAndNextAttrMaterialized = getPrevAndNextAttrMaterialized(
      attrsWithAccessor,
      ((attr:Attr) => outputRelation.attrNames.contains(attr)))
    val prevAndNextAttrAggregated = getPrevAndNextAttrMaterialized(
      attrsWithAccessor,
      ((attr:Attr) => joinAggregates.get(attr).isDefined))

    attrsWithAccessor.zip(prevAndNextAttrMaterialized.zip(prevAndNextAttrAggregated)).flatMap(attrAndPrevNextInfo => {
      val (attr, prevNextInfo) = attrAndPrevNextInfo
      val (materializedInfo, aggregatedInfo) = prevNextInfo
      val accessor = getAccessor(attr)
      if (accessor.isEmpty) {
        None // this should not happen
      } else {
        Some(new QueryPlanNPRRInfo(
          attr,
          accessor,
          outputRelation.attrNames.contains(attr),
          hasSelection(attr),
          getNextAnnotatedForLastMaterialized(attr, joinAggregates),
          getAggregation(joinAggregates, attr, aggregatedInfo),
          materializedInfo._1,
          materializedInfo._2))
      }
    })
  }

  private def getPrevAndNextAttrMaterialized(attrsWithAccessor: List[Attr],
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

  private def getNextAnnotatedForLastMaterialized(attr:Attr, joinAggregates:Map[String,ParsedAggregate]): Option[Attr] = {
    if (!outputRelation.attrNames.isEmpty && outputRelation.attrNames.last == attr) {
      attributeOrdering.dropWhile(a => a != attr).tail.find(a => joinAggregates.contains(a) && attrSet.contains(a))
    } else {
      None
    }
  }

  private def getAggregation(joinAggregates:Map[String,ParsedAggregate], attr:Attr, prevNextInfo:(Option[Attr], Option[Attr])): Option[QueryPlanAggregation] = {
   joinAggregates.get(attr).map(parsedAggregate => {
      new QueryPlanAggregation(parsedAggregate.op, parsedAggregate.init, parsedAggregate.expression, prevNextInfo._1, prevNextInfo._2)
    })
  }

  /**
   * @param attr, an attribute that definitely exists in this bag
   * @return Boolean for whether this attribute has a selection on it or not
   */
  private def hasSelection(attr:Attr): Boolean = {
    !attrToRels.get(attr).getOrElse(List())
      .filter(rel => !rel.attrs.filter(attrInfo => attrInfo._1 == attr).head._2.isEmpty).isEmpty
  }

  private def getOrderedAttrsWithAccessor(): List[Attr] = {
    attributeOrdering.flatMap(attr => {
      val accessor = getAccessor(attr)
      if (accessor.isEmpty) {
        None
      } else {
        Some(attr)
      }
    })
  }

  private def getAccessor(attr:Attr): List[QueryPlanAccessor] = {
    attrToRels.get(attr).getOrElse(List()).map(rel => {
      new QueryPlanAccessor(rel.name, rel.attrNames,(rel.attrNames.tail == attr && rel.annotationType != "void*"))
    })
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
        rels }
      else {
        subtreeRels
      }

    val distinctRelationNames = relsToUse.map(r => r.name).distinct
    distinctRelationNames.flatMap(n => {
      val relationsWithName = relsToUse.filter(r => {r.name == n})
      val orderingsAndRels: List[(List[Int], List[QueryRelation])] = relationsWithName.map(rn => {
        (GHD.getNumericalOrdering(attributeOrdering, rn), rn)
      }).groupBy(p => p._1).toList.map(elem => {
        (elem._1, elem._2.unzip._2)
      })

      orderingsAndRels.map(orderingAndRels => {
        val ordering = orderingAndRels._1
        val rels = orderingAndRels._2
        if (forTopLevelSummary) {
          new QueryPlanRelationInfo(rels.head.name, ordering, None, rels.head.annotationType)
        } else {
          new QueryPlanRelationInfo(rels.head.name, ordering, Some(rels.map(rel => rel.attrNames)), rels.head.annotationType)
        }
      })
    })
  }

  def createAttrToRelsMapping : Unit = {
    attrToRels = attrSet.map(attr =>{
      val relevantRels = subtreeRels.filter(rel => {
        rel.attrNames.contains(attr)
      })
      (attr, relevantRels)
    }).toMap
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
  def computeProjectedOutAttrsAndOutputRelation(outputAttrs:Set[Attr], attrsFromAbove:Set[Attr]): QueryRelation = {
    projectedOutAttrs = attrSet -- (outputAttrs ++ attrsFromAbove)
    val keptAttrs = attrSet intersect (outputAttrs ++ attrsFromAbove)
    // Right now we only allow a query to have one type of annotation, so
    // we take the annotation type from an arbitrary relation that was joined in this bag
    outputRelation = new QueryRelation(bagName, keptAttrs.map(attr =>(attr, "", "")).toList, rels.head.annotationType)
    val childrensOutputRelations = children.map(child => {
      child.computeProjectedOutAttrsAndOutputRelation(outputAttrs, attrsFromAbove ++ attrSet)
    })
    subtreeRels ++= childrensOutputRelations
    return outputRelation
  }

  def computeDepth : Unit = {
    if (children.isEmpty) {
      depth = 0
    } else {
      val childrenDepths = children.map(x => {
        x.computeDepth
        x.depth
      })
      depth = childrenDepths.foldLeft(0)((acc:Int, x:Int) => {
        if (x > acc) x else acc
      })
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
