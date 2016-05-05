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

  private def createAttrWithSelectionInfo(attr:Attr,
                                          attrToSelection:Map[Attr, Array[Selection]]): (Attr, Option[SelectionOp], Option[SelectionVal]) = {

    val attrName = attr
    val selectionsOpt = attrToSelection.get(attr)
    if (selectionsOpt.isDefined && !selectionsOpt.get.isEmpty) {
      val selections = selectionsOpt.get
      (attr, Some(selections(0).operation.value), Some(selections(0).value.toString))
    } else {
      (attr, None, None)
    }
  }

  private def aggregatesAreAnalogous(attr1:Attr,
                                     attr2:Attr,
                                     joinAggregates1:Map[String, Aggregation],
                                     joinAggregates2:Map[String, Aggregation]): Boolean = {
    val agg1Opt = joinAggregates1.get(attr1)
    val agg2Opt = joinAggregates2.get(attr2)
    agg1Opt match {
      case None => {
        if (agg2Opt.isEmpty) {
          true
        } else {
          false
        }
      }
      case Some(agg1) => {
        if (agg2Opt.isEmpty) {
          false
        } else {
          val agg2 = agg2Opt.get
          agg2.operation == agg1.operation &&
            agg2.init == agg1.init &&
            agg2.expression == agg1.expression
        }
      }
    }
  }
  /**
   * Try to match rel1 and rel2; in order for this to work, the attribute mappings such a map would imply must not contradict the
   * mappings we already know about. If this does work, return the new attribute mapping (which potentially has some new entries),
   * otherwise return None
   */
  def attrNameAgnosticRelationEquals(output1:Rel,
                                     rel1:Rel,
                                     output2:Rel,
                                     rel2:Rel,
                                     attrMap: Map[Attr, Attr],
                                     attrToSelection1: Map[Attr, Array[Selection]],
                                     attrToSelection2: Map[Attr, Array[Selection]],
                                     joinAggregates1:Map[String, Aggregation],
                                     joinAggregates2:Map[String, Aggregation]): Option[Map[Attr, Attr]] = {
    /* Rel names must match */
    if (!rel1.name.equals(rel2.name)) {
      return None
    }

    val zippedAttrs = (
      rel1.attrs.values.map(attr => createAttrWithSelectionInfo(attr, attrToSelection1)),
      rel1.attrs.values.map(attr => attrMap.get(attr)),
      rel2.attrs.values.map(attr => createAttrWithSelectionInfo(attr, attrToSelection2))).zipped.toList
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
          if (output1.attrs.values.contains(attrsTriple._1._1)!=output2.attrs.values.contains(attrsTriple._3._1)
            || !aggregatesAreAnalogous(attrsTriple._1._1, attrsTriple._3._1, joinAggregates1, joinAggregates2)) {
            None
          } else {
            Some(m.get + (attrsTriple._1._1 -> attrsTriple._3._1))
          }
        }
      })
    }
  }
}
