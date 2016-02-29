package duncecap

import duncecap.attr._

import scala.collection.mutable

object AttrOrderingUtil {

  /**
   * Partition attrNames into attrs with equality selection, then attrs without
   * keeping the ordering between attrs with equality selection,
   * keeping the ordering and between attrs without
   */
  def partition_equality_selected(attrs:List[Attr],
                                  selections:List[Selection]): List[Attr] = {
    val (hasSelect, noSelect) = attrs.partition(attr => selections.exists(selection => selection.attr == attr))
    return hasSelect:::noSelect
  }

  def partition_materialized(attrNames:List[Attr], outputRelation:Rel): List[Attr] = {
    val (materialized, notMaterialized) = attrNames.partition(outputRelation.attrs.values.contains(_))
    materialized:::notMaterialized
  }

  private def get_attribute_ordering(f_in:mutable.Set[EHNode],
                             outputRelation:Rel): List[String] = {
    var frontier = f_in
    var next_frontier = mutable.Set[EHNode]()
    var attr = scala.collection.mutable.ListBuffer.empty[String]

    while(frontier.size != 0){
      next_frontier.clear
      val level_attr = scala.collection.mutable.ListBuffer.empty[String]
      frontier.foreach{ cur:EHNode =>
        val cur_attrs = partition_materialized(cur.rels.flatMap{r => r.attrs.values}.sorted, outputRelation).distinct

        //collect others
        cur_attrs.foreach{ a =>
          if(!attr.contains(a) && !level_attr.contains(a)){
            level_attr += a
          }
        }

        cur.children.foreach{(child:GHDNode) =>
          next_frontier += child
        }
      }

      attr ++= level_attr

      var tmp = frontier
      frontier = next_frontier
      next_frontier = tmp
    }
    return attr.toList
  }

  def getAttributeOrdering(node:EHNode,
                           queryRelations:List[OptimizerRel],
                           outputRelation:Rel,
                           selections:List[Selection]) : List[String] = {
    val ordering = get_attribute_ordering(
      mutable.LinkedHashSet[EHNode](node),
      outputRelation
    )
    return partition_equality_selected(ordering, selections)
  }
}
