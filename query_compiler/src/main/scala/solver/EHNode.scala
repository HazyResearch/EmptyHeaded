package DunceCap

import DunceCap.attr._

import scala.collection.immutable.TreeSet

abstract class EHNode(val rels: List[QueryRelation]) {
  val attrSet = rels.foldLeft(TreeSet[String]())(
    (accum: TreeSet[String], rel: QueryRelation) => accum | TreeSet[String](rel.attrNames: _*))

  var attrToRels:Map[Attr,List[QueryRelation]] = PlanUtil.createAttrToRelsMapping(attrSet, rels)
  var attrToSelection:Map[Attr,List[QueryPlanSelection]] = attrSet.map(attr => (attr, PlanUtil.getSelection(attr, attrToRels))).toMap
  var outputRelation:QueryRelation = null
  var attributeOrdering: List[Attr] = null
  var children: List[GHDNode] = List()

  def setAttributeOrdering(ordering: List[Attr] )

  protected def getSelection(attr:Attr): List[QueryPlanSelection] = {
    attrToSelection.getOrElse(attr, List())
  }

  def getAccessor(attr:Attr): List[QueryPlanAccessor] = {
    PlanUtil.getAccessor(attr, attrToRels, attributeOrdering)
  }

  def getSelectedAttrs(): Iterable[Attr] = {
    attrToSelection.filter({case (attr, selects) => !selects.isEmpty}).keys
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
