package DunceCap

import DunceCap.attr.{Attr, SelectionVal, SelectionOp}

import scala.collection.mutable

object GHDSolver {
  def getAttrSet(rels: List[QueryRelation]): Set[String] = {
    return rels.foldLeft(Set[String]())(
      (accum: Set[String], rel : QueryRelation) => accum | rel.attrNames.toSet[String])
  }

  private def bottom_up(seen: mutable.Set[GHDNode], curr: GHDNode, parent:GHDNode, fn:((GHDNode,Boolean,GHDNode) => Unit) ): Unit = {
    val root = if(seen.size == 1) true else false
    for (child <- curr.children) {
      if (!seen.contains(child)) {
        seen += child
        bottom_up(seen, child, curr, fn)
      }
    }
    fn(curr,root,parent)
  }

  def bottomUp(curr: GHDNode, fn:((GHDNode,Boolean,GHDNode) => Unit) ): Unit = {
    bottom_up(mutable.LinkedHashSet[GHDNode](curr), curr, curr, fn)
  }

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

  def get_attribute_ordering(seen: mutable.Set[GHDNode],
                                     f_in:mutable.Set[GHDNode],
                                     resultAttrs:List[String]): List[String] = {
    var depth = 0
    var frontier = f_in
    var next_frontier = mutable.Set[GHDNode]()
    var attr = scala.collection.mutable.ListBuffer.empty[String]

    while(frontier.size != 0){
      next_frontier.clear
      val level_attr = scala.collection.mutable.ListBuffer.empty[String]
      frontier.foreach{ cur:GHDNode =>
        val cur_attrs = cur.rels.flatMap{r => r.attrNames}.sorted.distinct

        //collect others
        cur_attrs.foreach{ a =>
          if(!attr.contains(a) && !level_attr.contains(a)){
            level_attr += a
          }
        }

        cur.children.foreach{(child:GHDNode) =>
          if(!seen.contains(child)){
            seen += child
            next_frontier += child
          }
        }
      }

      //put those in the result first
      // materialized attrs first, in the order that they're in the materialized result
      val cur_attrs_sorted = level_attr.sortBy(e => if(resultAttrs.contains(e)) resultAttrs.indexOf(e) else resultAttrs.size+1).sorted
      cur_attrs_sorted.foreach{ a =>
        if(!attr.contains(a)){
          attr += a
        }
      }

      var tmp = frontier
      frontier = next_frontier
      next_frontier = tmp

      depth += 1
    }
    return attr.toList
  }

  private def breadth_first(seen: mutable.Set[GHDNode], f_in:mutable.Set[GHDNode]): (Int,Int) = {
    var depth = 0
    var frontier = f_in
    var next_frontier = mutable.Set[GHDNode]()
    while(frontier.size != 0){
      next_frontier.clear
      frontier.foreach{ cur:GHDNode =>
        cur.children.foreach{(child:GHDNode) =>
          if(!seen.contains(child)){
            seen += child
            next_frontier += child
          }
        }
      }

      var tmp = frontier
      frontier = next_frontier
      next_frontier = tmp

      depth += 1
    }
    return (depth,seen.size)
  }

  def getAttributeOrdering(myghd:GHDNode, queryRelations: List[QueryRelation], resultAttrs:List[String]) : List[String] ={
    val ordering = get_attribute_ordering(mutable.LinkedHashSet[GHDNode](myghd),mutable.LinkedHashSet[GHDNode](myghd),resultAttrs)
    partition_equality_selected(ordering, queryRelations.flatMap(queryRelation => queryRelation.attrs))
  }

  private def getConnectedComponents(rels: mutable.Set[QueryRelation], comps: List[List[QueryRelation]], ignoreAttrs: Set[String]): List[List[QueryRelation]] = {
    if (rels.isEmpty) return comps
    val component = getOneConnectedComponent(rels, ignoreAttrs)
    return getConnectedComponents(rels, component::comps, ignoreAttrs)
  }

  private def getOneConnectedComponent(rels: mutable.Set[QueryRelation], ignoreAttrs: Set[String]): List[QueryRelation] = {
    val curr = rels.head
    rels -= curr
    return DFS(mutable.LinkedHashSet[QueryRelation](curr), curr, rels, ignoreAttrs)
  }

  private def DFS(seen: mutable.Set[QueryRelation], curr: QueryRelation, rels: mutable.Set[QueryRelation], ignoreAttrs: Set[String]): List[QueryRelation] = {
    for (rel <- rels) {
      // if these two hyperedges are connected
      if (!((curr.attrNames.toSet[String] & rel.attrNames.toSet[String]) &~ ignoreAttrs).isEmpty) {
        seen += curr
        rels -= curr
        DFS(seen, rel, rels, ignoreAttrs)
      }
    }
    return seen.toList
  }

  // Visible for testing
  def getPartitions(leftoverBags: List[QueryRelation], // this cannot contain chosen
                    chosen: List[QueryRelation],
                    parentAttrs: Set[String],
                    tryBagAttrSet: Set[String]): Option[List[List[QueryRelation]]] = {
    // first we need to check that we will still be able to satisfy
    // the concordance condition in the rest of the subtree
    for (bag <- leftoverBags.toList) {
      if (!(bag.attrNames.toSet[String] & parentAttrs).subsetOf(tryBagAttrSet)) {
        return None
      }
    }

    // if the concordance condition is satisfied, figure out what components you just
    // partitioned your graph into, and do ghd on each of those disconnected components
    val relations = mutable.LinkedHashSet[QueryRelation]() ++ leftoverBags
    return Some(getConnectedComponents(relations, List[List[QueryRelation]](), getAttrSet(chosen).toSet[String]))
  }

  /**
   * @param partitions
   * @param parentAttrs
   * @return Each list in the returned list could be the children of the parent that we got parentAttrs from
   */
  private def getListsOfPossibleSubtrees(partitions: List[List[QueryRelation]], parentAttrs: Set[String]): List[List[GHDNode]] = {
    assert(!partitions.isEmpty)
    val subtreesPerPartition: List[List[GHDNode]] = partitions.map((l: List[QueryRelation]) => getDecompositions(l, parentAttrs))

    val foldFunc: (List[List[GHDNode]], List[GHDNode]) => List[List[GHDNode]]
    = (accum: List[List[GHDNode]], subtreesForOnePartition: List[GHDNode]) => {
      accum.map((children : List[GHDNode]) => {
        subtreesForOnePartition.map((subtree : GHDNode) => {
          subtree::children
        })
      }).flatten
    }

    return subtreesPerPartition.foldLeft(List[List[GHDNode]](List[GHDNode]()))(foldFunc)
  }

  private def getDecompositions(rels: List[QueryRelation], parentAttrs: Set[String]): List[GHDNode] =  {
    val treesFound = mutable.ListBuffer[GHDNode]()
    for (tryNumRelationsTogether <- (1 to rels.size).toList) {
      for (bag <- rels.combinations(tryNumRelationsTogether).toList) {
        val leftoverBags = rels.toSet[QueryRelation] &~ bag.toSet[QueryRelation]
        if (leftoverBags.toList.isEmpty) {
          val newNode = new GHDNode(bag)
          treesFound.append(newNode)
        } else {
          val bagAttrSet = getAttrSet(bag)
          val partitions = getPartitions(leftoverBags.toList, bag, parentAttrs, bagAttrSet)
          if (partitions.isDefined) {
            // lists of possible children for |bag|
            val possibleSubtrees: List[List[GHDNode]] = getListsOfPossibleSubtrees(partitions.get, bagAttrSet)
            for (subtrees <- possibleSubtrees) {
              val newNode = new GHDNode(bag)
              newNode.children = subtrees
              treesFound.append(newNode)
            }
          }
        }
      }
    }
    return treesFound.toList
  }

  def getDecompositions(rels: List[QueryRelation]): List[GHDNode] = {
    return getDecompositions(rels, Set[String]())
  }

  def getMinFHWDecompositions(rels: List[QueryRelation]): List[GHDNode] = {
    val decomps = getDecompositions(rels)
    val fhwsAndDecomps = decomps.map((root : GHDNode) => (root.fractionalScoreTree(), root))
    val minScore = fhwsAndDecomps.unzip._1.min

    case class Precision(val p:Double)
    class withAlmostEquals(d:Double) {
      def ~=(d2:Double)(implicit p:Precision) = (d-d2).abs <= p.p
    }
    implicit def add_~=(d:Double) = new withAlmostEquals(d)
    implicit val precision = Precision(0.001)

    val minFhws = fhwsAndDecomps.filter((scoreAndNode : (Double, GHDNode)) => scoreAndNode._1 ~= minScore)
    return minFhws.unzip._2
  }
}