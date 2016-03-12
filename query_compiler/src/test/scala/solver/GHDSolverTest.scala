package duncecap

import org.scalatest.FunSuite

class GHDSolverTest extends FunSuite {

  final val RELATIONS: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRel("a", "b", "c"),
    OptimizerRelFactory.createOptimizerRel("g", "a"),
    OptimizerRelFactory.createOptimizerRel("c", "d", "e"),
    OptimizerRelFactory.createOptimizerRel("e", "f"))
  final val PATH2: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRel("a", "b"),
    OptimizerRelFactory.createOptimizerRel("b", "c"))
  final val TADPOLE: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRel("a", "b"),
    OptimizerRelFactory.createOptimizerRel("b", "c"),
    OptimizerRelFactory.createOptimizerRel("c", "a"),
    OptimizerRelFactory.createOptimizerRel("a", "e"))
  final val SPLIT: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRel("b", "c", "d", "z", "y"),
    OptimizerRelFactory.createOptimizerRel("c", "d", "e", "i", "j"),
    OptimizerRelFactory.createOptimizerRel("b", "d", "f", "g", "h"),
    OptimizerRelFactory.createOptimizerRel("f", "g", "h", "k", "b"),
    OptimizerRelFactory.createOptimizerRel("f", "g", "h", "n", "b"))
  final val BARBELL: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRel("a", "b"),
    OptimizerRelFactory.createOptimizerRel("b", "c"),
    OptimizerRelFactory.createOptimizerRel("a", "c"),
    OptimizerRelFactory.createOptimizerRel("d", "e"),
    OptimizerRelFactory.createOptimizerRel("e", "f"),
    OptimizerRelFactory.createOptimizerRel("d", "f"),
    OptimizerRelFactory.createOptimizerRel("c", "d")
  )
  final val LONG_BARBELL: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRel("a", "b"),
    OptimizerRelFactory.createOptimizerRel("b", "c"),
    OptimizerRelFactory.createOptimizerRel("a", "c"),
    OptimizerRelFactory.createOptimizerRel("d", "e"),
    OptimizerRelFactory.createOptimizerRel("e", "f"),
    OptimizerRelFactory.createOptimizerRel("d", "f"),
    OptimizerRelFactory.createOptimizerRel("c", "p"),
    OptimizerRelFactory.createOptimizerRel("d", "p")
  )
  final val FFT: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRel("y0", "y1"),
    OptimizerRelFactory.createOptimizerRel("x0", "y0"),
    OptimizerRelFactory.createOptimizerRel("x0", "y1"),
    OptimizerRelFactory.createOptimizerRel("x1", "y0")
  )
  final val solver = GHDSolver

  test("Can form 3 node AJAR GHD for barbell") {
    val ajarGHDs = GHDSolver.computeAJAR_GHD(BARBELL.toSet, Set("a", "b", "c", "d", "e", "f"), Array())

    val singleNodeG_0Trees = ajarGHDs.filter(ghd => {
      ghd.attrSet.equals(Set("c", "d")) &&
        ghd.children.size == 2 &&
        (ghd.children.head.attrSet.equals(Set("d", "e", "f")) &&
        ghd.children.tail.head.attrSet.equals(Set("a", "b", "c"))) ||
        (ghd.children.head.attrSet.equals(Set("a", "b", "c")) &&
          ghd.children.tail.head.attrSet.equals(Set("d", "e", "f")))
    })

    assert(singleNodeG_0Trees.filter(ghd => ghd.children.head.children.isEmpty && ghd.children.tail.head.children.isEmpty).size == 1)
  }

  test("Finds an expected decomp of the barbell query") {
    // make sure that we get partition correctly after we choose the root
    val chosen = List(BARBELL.last)
    val partitions = solver.getPartitions(
      BARBELL.take(6),
      chosen,
      Set(),
      Set(),
      solver.getAttrSet(chosen))
    assert(partitions.isDefined)
    assert(partitions.get.size == 2)

    // filtering to look for expectedDecomp with one edge as root, and two triangles as children
    val decompositions = solver.getDecompositions(BARBELL, None, Array())
    var expectedDecomp = decompositions.filter((root : GHDNode) => root.rels.size == 1
      && root.rels.contains(BARBELL.last)
      && root.children.size == 2)
    expectedDecomp = expectedDecomp.filter((root : GHDNode) =>
      (root.children(0).attrSet.equals(Set("a", "b", "c")) && root.children(1).attrSet.equals(Set("d", "e", "f")))
        || root.children(1).attrSet.equals(Set("a", "b", "c")) && root.children(0).attrSet.equals(Set("d", "e", "f")))
    expectedDecomp = expectedDecomp.filter((root : GHDNode) => root.children(0).rels.size == 3 && root.children(1).rels.size == 3)

    assert(expectedDecomp.size >= 1)
  }

  test("Can form 1 node AJAR GHD for length 2 path query") {
    val ajarGHDs = GHDSolver.computeAJAR_GHD(PATH2.toSet, Set("a", "c"), Array())
    ajarGHDs.map(ajarGHD => {
      assert((Set("a", "b") == ajarGHD.attrSet &&
        ajarGHD.children.size == 1 &&
        Set("b", "c") == ajarGHD.children.head.attrSet) ||
        (Set("b", "c") == ajarGHD.attrSet &&
          ajarGHD.children.size == 1 &&
          Set("a", "b") == ajarGHD.children.head.attrSet)
      )
    })
  }

  test("check that ajar works correctly on query without aggregation as well") {
    val ajarGHDs = GHDSolver.computeAJAR_GHD(PATH2.toSet, Set("a", "b", "c"), Array())
    ajarGHDs.map(ajarGHD => {
      assert((Set("a", "b") == ajarGHD.attrSet &&
        ajarGHD.children.size == 1 &&
        Set("b", "c") == ajarGHD.children.head.attrSet) ||
        (Set("b", "c") == ajarGHD.attrSet &&
          ajarGHD.children.size == 1 &&
          Set("a", "b") == ajarGHD.children.head.attrSet))
    })
  }

  test("Can identify connected components of graph when removing the chosen hyper edge leaves 2 disconnected components") {
    val chosen = List(RELATIONS.head)
    val partitions = solver.getPartitions(
      RELATIONS.tail, chosen, Set(), Set(), solver.getAttrSet(chosen))
    assert(partitions.isDefined)
    assert(partitions.get.size == 2)

    val firstPart = partitions.get.head
    val secondPart = partitions.get.tail.head
    assert(firstPart.size == 1 && secondPart.size == 2)
    assert(firstPart.head == RELATIONS(1))
    assert(secondPart.head == RELATIONS(2))
    assert(secondPart.tail.head == RELATIONS(3))
  }

  test("Finds all possible decompositions of len 2 path query)") {
    val decompositions = solver.getDecompositions(PATH2, None, Array()).toSet[GHDNode]
    /**
     * The decompositions we expect are [ABC] and [AB]--[BC] and [BC]--[AB]
     */
    assert(decompositions.size == 3)
    val singleBag = new GHDNode(PATH2, Array())
    val twoBagWithRootAB = new GHDNode(PATH2.take(1),Array())
    twoBagWithRootAB.children = List(new GHDNode(PATH2.tail.take(1),Array()))
    val twoBagWithRootBC = new GHDNode(PATH2.tail.take(1),Array())
    twoBagWithRootBC.children = List(new GHDNode(PATH2.take(1),Array()))
    assert(decompositions.contains(singleBag))
    assert(decompositions.contains(twoBagWithRootAB))
    assert(decompositions.contains(twoBagWithRootBC))
  }

  test("Decomps and scores triangle query correctly") {
    val decompositions = solver.getDecompositions(TADPOLE.take(3), None, Array()) // drop the tail
    val fractionalScores = decompositions.map((root: GHDNode) => root.fractionalScoreTree())
    assert(fractionalScores.min === 1.5)
  }

  test("Find max bag size 5 decomposition of query") {
    val decompositions2 = solver.getDecompositions(SPLIT, None, Array())
    assert(!decompositions2.filter((root: GHDNode) => root.scoreTree <= 5).isEmpty)
  }


  test("Finds all possible decompositions of tadpole query)") {
    val decompositions = solver.getDecompositions(TADPOLE, None, Array())
    val decompositionsSet = decompositions.toSet[GHDNode]
    /**
     * The decompositions we expect are
     * [AB]--[ABC] (*)
     *  |
     * [AE]
     *
     * root of above tree could also be AC
     *
     * [AB]--[ABCE]
     * [AC]--[ABCE]
     * [BC]--[ABCE]
     *
     * [ABC]--[AE]
     * [ABE]--[ABC] (*)
     * [ACE]--[ABC]
     * [AEBC]--[ABC]
     *
     * all of the above 2-node options also work if you switch the root and leaf
     *
     * [AE]--[AB]--[ABC]
     * [AE]--[ABC]--[AB]
     * [BC]--[ABC]--[AE]
     * [AE]--[ABC]--[BC]
     *
     * [ABCE] (*)
     *
     * Check that the ones marked (*) were found:
     */
    val decomp1 = new GHDNode(List(TADPOLE(0)), Array())
    val decomp1Child1 = new GHDNode(List(TADPOLE(1), TADPOLE(2)), Array())
    val decomp1Child2 = new GHDNode(List(TADPOLE(3)), Array())
    decomp1.children = List(decomp1Child1, decomp1Child2)
    assert(decompositionsSet.contains(decomp1))

    val decomp2 = new GHDNode(List(TADPOLE(0), TADPOLE(3)), Array())
    val decomp2Child = new GHDNode(List(TADPOLE(1), TADPOLE(2)), Array())
    decomp2.children = List(decomp2Child)
    assert(decompositionsSet.contains(decomp2))

    val decomp3 = new GHDNode(List(TADPOLE(0), TADPOLE(1), TADPOLE(2), TADPOLE(3)), Array())
    assert(decompositionsSet.contains(decomp3))

    // Also check that we found the lowest fhw option
    val decomp4 = new GHDNode(List(TADPOLE(3)), Array())
    val decomp4Child = new GHDNode(List(TADPOLE(0), TADPOLE(1), TADPOLE(2)), Array())
    decomp4.children = List(decomp4Child)
    assert(decompositionsSet.contains(decomp4))

    val fractionalScores = decompositions.map((root: GHDNode) => root.fractionalScoreTree())
    assert(fractionalScores.min === 1.5)
  }
}
