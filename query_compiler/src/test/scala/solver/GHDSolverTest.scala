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
    val ajarGHDs = GHDSolver.computeAJAR_GHD(BARBELL.toSet, Set("c", "d"), Array())

    ajarGHDs.map(println(_))

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
      assert(Set("a", "b", "c") == ajarGHD.attrSet)
      assert(ajarGHD.children.size == 0)
    })
  }

  test("check that ajar works correctly on query without aggregation as well") {
    val ajarGHDs = GHDSolver.computeAJAR_GHD(PATH2.toSet, Set("a", "b", "c"), Array())
    ajarGHDs.map(println(_))
    ajarGHDs.map(ajarGHD => {
      assert(Set("a", "b", "c") == ajarGHD.attrSet)
      assert(ajarGHD.children.size == 0)
    })
  }

  test("blah") {
    val ajarGHDs = GHDSolver.computeAJAR_GHD(BARBELL.toSet, Set("a", "b", "c", "d", "e", "f"), Array())
    ajarGHDs.map(println(_))

  }

  test("blah2") {
    val ajarGHDs = GHDSolver.computeAJAR_GHD(LONG_BARBELL.toSet, Set("a", "b", "c", "d", "e", "f"), Array(Selection("p", EQUALS(), "0")))
    ajarGHDs.map(println(_))
  }
}
