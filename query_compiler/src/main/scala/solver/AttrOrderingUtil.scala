package duncecap

import duncecap.attr._

import scala.collection.mutable

object AttrOrderingUtil {

  /**
   * Partition attrNames into attrs with equality selection, then attrs without
   * keeping the ordering between attrs with equality selection,
   * keeping the ordering between attrs without
   */
  def partition_equality_selected(attrs:List[Attr],
                                  selections:List[Selection]): List[Attr] = {
    val (hasSelect, noSelect) = attrs.partition(attr => selections.exists(selection => selection.attr == attr))
    return hasSelect:::noSelect
  }

  def partition_materialized(attrNames:List[Attr], outputRelation:RelBase): List[Attr] = {
    val (materialized, notMaterialized) = attrNames.partition(outputRelation.attrs.values.contains(_))
    materialized:::notMaterialized
  }

  def order_by_num_times_passed_up(attrNames:List[Attr], node:EHNode): List[Attr] = {
    val attrs = attrNames.toArray
    scala.util.Sorting.stableSort(attrs,
      (attr1:Attr, attr2:Attr) => node.children.count(child => child.attrSet.contains(attr1)) > node.children.count(child => child.attrSet.contains(attr2)))
    return attrs.toList
  }

  private def get_attribute_ordering(f_in:mutable.Set[EHNode],
                             outputRelation:RelBase): List[String] = {
    var frontier = f_in
    var next_frontier = mutable.Set[EHNode]()
    var attr = scala.collection.mutable.ListBuffer.empty[String]

    while(frontier.size != 0){
      next_frontier.clear
      val level_attr = scala.collection.mutable.ListBuffer.empty[String]
      frontier.foreach{ cur:EHNode =>
        val cur_attrs = partition_materialized(cur.rels.flatMap{r => r.attrs.values}.sorted, outputRelation).distinct

        //collect others
        val filtered_cur_attr = cur_attrs.filter{ a =>
          (!attr.contains(a) && !level_attr.contains(a))
        }

        level_attr ++= order_by_num_times_passed_up(filtered_cur_attr, cur)

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
                           outputRelation:RelBase,
                           selections:List[Selection]) : List[String] = {
    val ordering = get_attribute_ordering(
      mutable.LinkedHashSet[EHNode](node),
      outputRelation
    )
    return partition_equality_selected(ordering, selections)
  }
}
