package duncecap

import duncecap.attr._

object PlanUtil {
  def createAttrToRelsMapping(attrs:Set[Attr], rels:List[OptimizerRel]): Map[Attr, List[OptimizerRel]] = {
    attrs.map(attr => {
      val relevantRels = rels.filter(rel => {
        rel.attrs.values.contains(attr)
      })
      (attr, relevantRels)
    }).toMap
  }

  def getSelection(attr:Attr, selections:Array[Selection]) = {
    selections.filter(selection => selection.getAttr() == attr)
  }

  /*
  def reorderByNumericalOrdering(attr:List[Attr], ordering:List[Int]): List[Attr] = {
    ordering.map(o => attr(o))
  }

  def getNumericalOrdering(attributeOrdering:List[Attr], rel:OptimizerRel): List[Int] = {
    attributeOrdering.map(a => rel.attrs.values.indexOf(a)).filter(pos => {
      pos != -1
    })
  }

  def getOrderedAttrsWithAccessor(attributeOrdering:List[Attr], attrToRels:Map[Attr, List[OptimizerRel]]): List[Attr] = {
    attributeOrdering.flatMap(attr => {
      val accessor = getAccessor(attr, attrToRels, attributeOrdering)
      if (accessor.isEmpty) {
        None
      } else {
        Some(attr)
      }
    })
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
  */
}
