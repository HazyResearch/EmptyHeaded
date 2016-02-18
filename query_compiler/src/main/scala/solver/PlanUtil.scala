package duncecap

import duncecap.attr._

object PlanUtil {
  def getRelationInfoBasedOnName(forTopLevelSummary: Boolean,
                                 relsToUse:List[QueryRelation],
                                 attributeOrdering:List[Attr]): List[QueryPlanRelationInfo] = {
    val distinctRelationNames = relsToUse.map(r => r.name).distinct

    distinctRelationNames.flatMap(n => {
      val relationsWithName = relsToUse.filter(r => {
        r.name == n
      })
      val orderingsAndRels: List[(List[Int], List[QueryRelation])] = relationsWithName.map(rn => {
        (PlanUtil.getNumericalOrdering(attributeOrdering, rn), rn)
      }).groupBy(p => p._1).toList.map(elem => {
        (elem._1, elem._2.unzip._2)
      })
      val or = orderingsAndRels.map(orderingAndRels => {
        val ordering = orderingAndRels._1
        val rels = orderingAndRels._2
        if (forTopLevelSummary) {
          new QueryPlanRelationInfo(rels.head.name, ordering, None, rels.head.annotationType)
        } else {
          new QueryPlanRelationInfo(rels.head.name, ordering, Some(rels.map(rel => reorderByNumericalOrdering(rel.attrNames, ordering))), rels.head.annotationType)
        }
      })
      or
    })
  }

  def reorderByNumericalOrdering(attr:List[Attr], ordering:List[Int]): List[Attr] = {
    ordering.map(o => attr(o))
  }

  def getNumericalOrdering(attributeOrdering:List[Attr], rel:QueryRelation): List[Int] = {
    attributeOrdering.map(a => rel.attrNames.indexOf(a)).filter(pos => {
      pos != -1
    })
  }

  def getOrderedAttrsWithAccessor(attributeOrdering:List[Attr], attrToRels:Map[Attr, List[QueryRelation]]): List[Attr] = {
    attributeOrdering.flatMap(attr => {
      val accessor = getAccessor(attr, attrToRels, attributeOrdering)
      if (accessor.isEmpty) {
        None
      } else {
        Some(attr)
      }
    })
  }

  def getAccessor(attr:Attr, attrToRels:Map[Attr, List[QueryRelation]], attributeOrdering:List[Attr]): List[QueryPlanAccessor] = {
    attrToRels.get(attr).getOrElse(List()).map(rel => {
      val ordering = getNumericalOrdering(attributeOrdering, rel)
      val reordered = reorderByNumericalOrdering(rel.attrNames, ordering)
      new QueryPlanAccessor(
        rel.name,
        reordered,
        (reordered.last == attr && rel.annotationType != "void*"))
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
      .map(attrInfo => QueryPlanSelection(attrInfo._2, attrInfo._3)).distinct
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
