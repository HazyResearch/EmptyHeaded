package DunceCap

import DunceCap.attr._

import scala.collection.mutable

/**
 * Created by sctu on 12/1/15.
 */
object AttrOrderingUtil {

  /**
   * Partition attrNames into attrs with equality selection, then attrs without
   * keeping the ordering between attrs with equality selection,
   * keeping the ordering and between attrs without
   */
  def partition_equality_selected(attrNames:List[Attr],
                                  attrInfo:List[(Attr, SelectionOp, SelectionVal)]): List[Attr] = {
    val attrsWithEqualitySelection = attrInfo.filter(info => info._2 == "=").unzip3._1.toSet
    val (attrsWithEqSelect, attrsWithoutEqSelect) = attrNames.partition(
      attrName => attrsWithEqualitySelection.contains(attrName))
    attrsWithEqSelect:::attrsWithoutEqSelect
  }

  def partition_materialized(attrNames:List[Attr], outputRelation:QueryRelation): List[Attr] = {
    val (materialized, notMaterialized) = attrNames.partition(outputRelation.attrNames.contains(_))
    materialized:::notMaterialized
  }

  private def get_attribute_ordering(f_in:mutable.Set[EHNode],
                             outputRelation:QueryRelation): List[String] = {
    var frontier = f_in
    var next_frontier = mutable.Set[EHNode]()
    var attr = scala.collection.mutable.ListBuffer.empty[String]

    while(frontier.size != 0){
      next_frontier.clear
      val level_attr = scala.collection.mutable.ListBuffer.empty[String]
      frontier.foreach{ cur:EHNode =>
        val cur_attrs = partition_materialized(cur.rels.flatMap{r => r.attrNames}.sorted, outputRelation).distinct

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
                           queryRelations:List[QueryRelation],
                           outputRelation:QueryRelation) : List[String] = {
    val ordering = get_attribute_ordering(
      mutable.LinkedHashSet[EHNode](node),
      outputRelation
    )
    partition_equality_selected(ordering, queryRelations.flatMap(queryRelation => queryRelation.attrs))
  }
}
