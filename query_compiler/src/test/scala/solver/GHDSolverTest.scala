package duncecap

import org.scalatest.FunSuite

class GHDSolverTest extends FunSuite {

  final val RELATIONS: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRelWithNoSelects("a", "b", "c"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("g", "a"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("c", "d", "e"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("e", "f"))
  final val PATH2: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRelWithNoSelects("a", "b"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("b", "c"))
  final val TADPOLE: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRelWithNoSelects("a", "b"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("b", "c"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("c", "a"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("a", "e"))
  final val SPLIT: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRelWithNoSelects("b", "c", "d", "z", "y"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("c", "d", "e", "i", "j"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("b", "d", "f", "g", "h"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("f", "g", "h", "k", "b"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("f", "g", "h", "n", "b"))
  final val BARBELL: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRelWithNoSelects("a", "b"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("b", "c"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("a", "c"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("d", "e"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("e", "f"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("d", "f"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("c", "d")
  )
  final val FFT: List[OptimizerRel] = List(
    OptimizerRelFactory.createOptimizerRelWithNoSelects("y0", "y1"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("x0", "y0"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("x0", "y1"),
    OptimizerRelFactory.createOptimizerRelWithNoSelects("x1", "y0")
  )
  final val solver = GHDSolver

  test("Can form 3 node AJAR GHD for barbell") {
    val ajarGHDs = GHDSolver.computeAJAR_GHD(BARBELL.toSet, Set("c", "d"), Array())

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
      BARBELL.take(6), chosen, Set(), solver.getAttrSet(chosen))
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
}
