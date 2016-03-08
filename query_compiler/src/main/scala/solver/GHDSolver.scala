package duncecap

import scala.collection.mutable

object GHDSolver {
  def computeAJAR_GHD(rels: Set[OptimizerRel], output: Set[String], selections:Array[Selection]):List[GHDNode] = {
    val components = getConnectedComponents(
      mutable.Set(rels.toList.filter(rel => !(rel.attrs.values.toSet subsetOf output)):_*), List(), output, Set())
    val componentsPlus = components.map(getAttrSet(_))
    val H_0_edges = rels.filter(rel => rel.attrs.values.toSet subsetOf output) union
      componentsPlus.map(compPlus => output intersect compPlus).map(OptimizerRel.createImaginaryOptimizerRelWithNoSelects(_)).toSet
    val characteristicHypergraphEdges = components.zip(componentsPlus).map({
      compAndCompPlus => getCharacteristicHypergraphEdges(compAndCompPlus._1.toSet, compAndCompPlus._2, output).toList
    })

    val G_i_options = getMinFHWDecompositions(H_0_edges.toList, selections)::characteristicHypergraphEdges
      .map(H => getMinFHWDecompositions(H.toList.filter(!_.isImaginary), selections, H.find(_.isImaginary)))

    val G_i_combos = G_i_options.foldLeft(List[List[GHDNode]](List[GHDNode]()))(allSubtreeAssignmentsFoldFunc)//.take(1) // need to make some copies here

    // These are the GHDs described in the AJAR paper;
    // We get rid of all the edges that don't correspond to relations
    // in order to get the GHD that we actually create our query plan from.
    // Note that the GHDs returned from this function might
    // therefore not actually conform to the definition of a GHD,
    // but we guarantee that the result of running yannakakis over it
    // is the same as if we had these imaginary edges that contain all possible tuples
    val theoreticalGHDs = G_i_combos.map(trees => {
      val reversedTrees = trees.reverse
      stitchTogether(duplicateTree(reversedTrees.head), reversedTrees.tail, componentsPlus, output)
    })
    theoreticalGHDs.flatMap(deleteImaginaryEdges(_))
  }


  /**
   * Delete the imaginary edges (added when constructing the characteristic hypergraphs) from the GHD
   * @param validGHD
   * @return
   */
  def deleteImaginaryEdges(validGHD: GHDNode): Option[GHDNode] = {
    val realEdges = validGHD.rels.filter(!_.isImaginary)
    if (realEdges.isEmpty) { // you'll have to delete this entire node
      val listOfProcessedChildren = validGHD.children.flatMap(deleteImaginaryEdges(_))
      if (listOfProcessedChildren.isEmpty) return None
      else {
        val newRoot = listOfProcessedChildren.head
        newRoot.children = newRoot.children:::listOfProcessedChildren.tail
        return Some(newRoot)
      }
    } else {
      val newGHD = new GHDNode(realEdges, validGHD.selections)
      newGHD.bagFractionalWidth = validGHD.bagFractionalWidth
      newGHD.children = validGHD.children.flatMap(deleteImaginaryEdges(_))
      return Some(newGHD)
    }
  }

  def stitchTogether(G_0:GHDNode,
                     G_i:List[GHDNode],
                     componentPlus: List[Set[String]],
                     agg:Set[String]): GHDNode = {
    val G_0_nodes = G_0.toList
    G_i.zip(componentPlus).foreach({ case (g_i, compPlus) => {
      val stitchable = G_0_nodes.find(node => {
        (agg intersect compPlus) subsetOf node.attrSet
      })
      assert(stitchable.isDefined) // in theory, we always find a stitchable node
      stitchable.get.children = g_i::stitchable.get.children
      assert((agg intersect compPlus) subsetOf g_i.attrSet)
    }})
    return G_0
  }

  def getCharacteristicHypergraphEdges(comp: Set[OptimizerRel], compPlus: Set[String], output: Set[String]): mutable.Set[OptimizerRel] = {
    val characteristicHypergraphEdges:mutable.Set[OptimizerRel] = mutable.Set[OptimizerRel](comp.toList:_*)
    characteristicHypergraphEdges += OptimizerRel.createImaginaryOptimizerRelWithNoSelects(compPlus intersect output)
    return characteristicHypergraphEdges
  }

  def getAttrSet(rels: List[OptimizerRel]): Set[String] = {
    return rels.foldLeft(Set[String]())(
      (accum: Set[String], rel:OptimizerRel) => accum | rel.attrs.values.toSet[String])
  }

  private def getConnectedComponents(rels: mutable.Set[OptimizerRel],
                                     comps: List[List[OptimizerRel]],
                                     ignoreAttrs: Set[String],
                                     reusable:Set[OptimizerRel]): List[List[OptimizerRel]] = {
    if (rels.isEmpty) return comps
    val (component, newReusable) = getOneConnectedComponent(rels, ignoreAttrs, reusable)
    return getConnectedComponents(rels, component::comps, ignoreAttrs, newReusable)
  }

  private def getOneConnectedComponent(rels: mutable.Set[OptimizerRel],
                                       ignoreAttrs: Set[String],
                                       reusable:Set[OptimizerRel]): (List[OptimizerRel], Set[OptimizerRel]) = {
    val curr = rels.toList.sortBy(rel => -rel.nonSelectedAttrNames.size).head
    rels -= curr
    val component = DFS(mutable.LinkedHashSet[OptimizerRel](curr), curr, rels, ignoreAttrs)
    val alsoCovered = getCoveredIfSelectsIgnored(component, rels, rels ++ reusable)
    return (component:::alsoCovered.toList, alsoCovered ++ reusable)
  }

  /**
   * This gets relations still in rels that are covered by a relation in your current component if you ignore selects
   *
   * This is correct (i.e., you don't miss lower fhw decomps) because for any decomps D that you could have
   * made with |component|, you can now have D', where D' is just D iwth a couple rels added into bags that already cover
   * them anyways
   *
   * This is potentially helpful because it gives you an opportunity to put rels with selections
   * lower in the GHD
   */
  private def getCoveredIfSelectsIgnored(component:List[OptimizerRel],
                                         shouldBeUpdatedRels: mutable.Set[OptimizerRel],
                                         rels: mutable.Set[OptimizerRel]): Set[OptimizerRel] = {
    var covered = mutable.Set[OptimizerRel]()
    val componentAttrs = component.flatMap(rel => rel.attrs.values).toSet
    for (rel <- rels.toList) {
      if (component.exists(c => rel.nonSelectedAttrNames subsetOf c.attrs.values.toSet)) {
        covered += rel
        rels -= rel
        shouldBeUpdatedRels -= rel
      }
    }
    return covered.toSet
  }

  private def DFS(seen: mutable.Set[OptimizerRel], curr: OptimizerRel, rels: mutable.Set[OptimizerRel], ignoreAttrs: Set[String]): List[OptimizerRel] = {
    for (rel <- rels.toList) {
      // if these two hyperedges are connected
      if (!((curr.attrs.values.toSet[String] & rel.attrs.values.toSet[String]) &~ ignoreAttrs).isEmpty) {
        seen += curr
        rels -= curr
        DFS(seen, rel, rels, ignoreAttrs)
      }
    }
    return seen.toList
  }

  // Visible for testing
  def getPartitions(leftoverBags: List[OptimizerRel], // this cannot contain chosen
                    chosen: List[OptimizerRel],
                    parentAttrs: Set[String],
                    tryBagAttrSet: Set[String]): Option[List[List[OptimizerRel]]] = {
    // first we need to check that we will still be able to satisfy
    // the concordance condition in the rest of the subtree
    for (bag <- leftoverBags.toList) {
      if (!(bag.attrs.values.toSet[String] & parentAttrs).subsetOf(tryBagAttrSet)) {
        return None
      }
    }

    // if the concordance condition is satisfied, figure out what components you just
    // partitioned your graph into, and do ghd on each of those disconnected components
    val relations = mutable.LinkedHashSet[OptimizerRel]() ++ leftoverBags
    return Some(getConnectedComponents(relations, List[List[OptimizerRel]](), getAttrSet(chosen).toSet[String], Set()))
  }

  /**
   * @param partitions
   * @param parentAttrs
   * @return Each list in the returned list could be the children of the parent that we got parentAttrs from
   */
  private def getListsOfPossibleSubtrees(partitions: List[List[OptimizerRel]],
                                         parentAttrs: Set[String],
                                         selections:Array[Selection]): List[List[GHDNode]] = {
    assert(!partitions.isEmpty)
    val subtreesPerPartition: List[List[GHDNode]] = partitions.map((l: List[OptimizerRel]) => getDecompositions(l, None, parentAttrs, selections))
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
    val newGHD = new GHDNode(ghd.rels, ghd.selections)
    newGHD.bagFractionalWidth = ghd.bagFractionalWidth
    newGHD.children = ghd.children.map(duplicateTree(_))
    return newGHD
  }

  private def bagCannotBeExpanded(bag: GHDNode, leftOverRels: Set[OptimizerRel]): Boolean = {
    // true if there does not exist a remaining rel entirely covered by this bag
    val b = leftOverRels.forall(rel => !rel.attrs.values.forall(attrName => bag.attrSet.contains(attrName)))
    return b
  }

  private def getDecompositions(rels: List[OptimizerRel],
                                imaginaryRel:Option[OptimizerRel],
                                parentAttrs:Set[String],
                                selections:Array[Selection]): List[GHDNode] =  {

    val treesFound = mutable.ListBuffer[GHDNode]()
    for (tryNumRelationsTogether <- (1 to rels.size).toList) {
      for (combo <- rels.combinations(tryNumRelationsTogether).toList) {
        val bag =
          if (imaginaryRel.isDefined) {
            imaginaryRel.get::combo
          } else {
            combo
          }
        // If your edges cover attributes that a larger set of edges could cover, then
        // don't bother trying this bag
        val leftoverBags = rels.toSet[OptimizerRel] &~ bag.toSet[OptimizerRel]
        val newNode = new GHDNode(bag, selections)
        if (bagCannotBeExpanded(newNode, leftoverBags)) {
          if (leftoverBags.toList.isEmpty) {
            treesFound.append(newNode)
          } else {
            val bagAttrSet = getAttrSet(bag)
            val partitions = getPartitions(leftoverBags.toList, bag, parentAttrs, bagAttrSet)
            if (partitions.isDefined) {
              // lists of possible children for |bag|
              val possibleSubtrees: List[List[GHDNode]] = getListsOfPossibleSubtrees(partitions.get, bagAttrSet, selections)
              for (subtrees <- possibleSubtrees) {
                newNode.children = subtrees
                treesFound.append(newNode)
              }
            }
          }
        }
      }
    }
    return treesFound.toList
  }

  def getDecompositions(rels: List[OptimizerRel], imaginaryRel:Option[OptimizerRel], selections:Array[Selection]): List[GHDNode] = {
    return getDecompositions(rels, imaginaryRel, Set[String](), selections)
  }

  def getMinFHWDecompositions(rels: List[OptimizerRel], selections:Array[Selection], imaginaryRel:Option[OptimizerRel] = None): List[GHDNode] = {
    val decomps = getDecompositions(rels, imaginaryRel, selections)
    val fhwsAndDecomps = decomps.map((root :GHDNode) => (root.fractionalScoreTree(), root))
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