package DunceCap

import DunceCap.attr.{Attr, SelectionVal, SelectionOp}

import scala.collection.mutable

object GHDSolver {
  def computeAJAR_GHD(rels: mutable.Set[QueryRelation], output: Set[String]):List[GHDNode] = {
    /*val components = getConnectedComponents(
      rels.map(rel => QueryRelationFactory.createQueryRelationWithNoSelects(rel.attrNames.filter(name => !output.contains(name)))).filter(rel => !rel.attrs.isEmpty),
      List(), Set())*/
    val components = getConnectedComponents(rels, List(), output).filter(rels => !(rels.size == 1 && rels.head.attrNames.toSet.equals(output)))
    println(components)
    val componentsPlus = components.map(getAttrSet(_))
    println(componentsPlus)
    val H_0_edges = rels.filter(rel => rel.attrNames.toSet subsetOf output) union
      componentsPlus.map(compPlus => output intersect compPlus).map(QueryRelationFactory.createQueryRelationWithNoSelects(_)).toSet
    val characteristicHypergraphEdges = components.zip(componentsPlus).map({
      compAndCompPlus => getCharacteristicHypergraphEdges(compAndCompPlus._1.toSet, compAndCompPlus._2, output).toList
    })

    println((H_0_edges::characteristicHypergraphEdges).size)
    val G_i_options = (H_0_edges::characteristicHypergraphEdges).map(H => getMinFHWDecompositions(H.toList))
    println("G_1 options")
    println(G_i_options(1))

    val G_i_combos = G_i_options.foldLeft(List[List[GHDNode]](List[GHDNode]()))(allSubtreeAssignmentsFoldFunc) // need to make some copies here
    println(G_i_combos.size) // 43
    G_i_combos.map(trees => {
      val reversedTrees = trees.reverse
      stitchTogether(duplicateTree(reversedTrees.head), reversedTrees.tail, componentsPlus, output)
    })
  }

  def generateAllPossiblePairs(l1:List[GHDNode], l2:List[GHDNode]): List[(GHDNode, GHDNode)] = {
    for {x <- l1; y <- l2} yield (x, y)
  }

  def stitchTogether(G_0:GHDNode,
                     G_i:List[GHDNode],
                     componentPlus: List[Set[Attr]],
                     agg:Set[String]): GHDNode = {
    G_i.zip(componentPlus).foreach({ case (g_i, compPlus) => {
      val stitchable = generateAllPossiblePairs(G_0.toList, g_i.toList).find(nodes => {
        (((agg intersect compPlus) subsetOf nodes._1.attrSet) &&
          ((agg intersect compPlus) subsetOf nodes._2.attrSet))
      })
      assert(stitchable.isDefined)
      stitchable.get._1.children = reroot(g_i, stitchable.get._2)::stitchable.get._1.children
    }})
    return G_0
  }

  def reroot(oldRoot:GHDNode, newRoot:GHDNode): GHDNode = {
   // val oldRoot = duplicateTree(oldRootRef)
    val path = getPathToNode(oldRoot, newRoot)
    path.map(p => println(p.attrSet))
    path.take(path.size-1).foldRight(path.last)((prev:GHDNode,next:GHDNode) => {
      next.children = prev::next.children
      prev.children = prev.children.filter(child => !(child eq next))
      prev
    })
    return path.head
  }

  def getPathToNode(root:GHDNode, n: GHDNode):List[GHDNode] = {
    if (root eq n) {
      return List(n)
    } else if (root.children.isEmpty) {
      return List()
    } else  {
        val pathsFromChildren = root.children.map(getPathToNode(_, n))
        root::pathsFromChildren.find(path => !path.isEmpty).get // todo error case
    }
  }

  def getCharacteristicHypergraphEdges(comp: Set[QueryRelation], compPlus: Set[String], agg: Set[String]): mutable.Set[QueryRelation] = {
    val characteristicHypergraphEdges:mutable.Set[QueryRelation] = mutable.Set[QueryRelation](comp.toList:_*)
    characteristicHypergraphEdges += QueryRelationFactory.createQueryRelationWithNoSelects(compPlus intersect agg)
    return characteristicHypergraphEdges
  }

  def getAttrSet(rels: List[QueryRelation]): Set[String] = {
    return rels.foldLeft(Set[String]())(
      (accum: Set[String], rel : QueryRelation) => accum | rel.attrNames.toSet[String])
  }

  private def getConnectedComponents(rels: mutable.Set[QueryRelation], comps: List[List[QueryRelation]], ignoreAttrs: Set[String]): List[List[QueryRelation]] = {
    if (rels.isEmpty) return comps
    val component = getOneConnectedComponent(rels, ignoreAttrs)
    return getConnectedComponents(rels, component::comps, ignoreAttrs)
  }

  private def ajarGetConnectedComponents(): Unit = {

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
    return subtreesPerPartition.foldLeft(List[List[GHDNode]](List[GHDNode]()))(allSubtreeAssignmentsFoldFunc)
  }

  private def allSubtreeAssignmentsFoldFunc(accum: List[List[GHDNode]], subtreesForOnePartition: List[GHDNode]): List[List[GHDNode]] = {
    accum.map((children : List[GHDNode]) => {
      subtreesForOnePartition.map((subtree : GHDNode) => {
        subtree::children
      })
    }).flatten
  }

  private def allSubtreeAssignmentsDeepCopyFoldFunc(accum: List[List[GHDNode]], subtreesForOnePartition: List[GHDNode]): List[List[GHDNode]] = {
    accum.map((children : List[GHDNode]) => {
      subtreesForOnePartition.map((subtree : GHDNode) => {
        duplicateTree(subtree)::children
      })
    }).flatten
  }

  private def duplicateTree(ghd: GHDNode): GHDNode = {
    val newGHD = new GHDNode(ghd.rels)
    newGHD.bagFractionalWidth = ghd.bagFractionalWidth
    newGHD.children = ghd.children.map(duplicateTree(_))
    return newGHD
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