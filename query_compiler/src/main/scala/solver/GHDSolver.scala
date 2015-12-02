package DunceCap

import DunceCap.attr.{Attr, SelectionVal, SelectionOp}

import scala.collection.mutable

object GHDSolver {
  def getAttrSet(rels: List[QueryRelation]): Set[String] = {
    return rels.foldLeft(Set[String]())(
      (accum: Set[String], rel : QueryRelation) => accum | rel.attrNames.toSet[String])
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