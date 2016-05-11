package duncecap

import org.scalatest.FunSuite

class GHDNodeTest extends FunSuite {
  test("Check that iterator works correctly on a two node GHD") {
    val PATH2: List[OptimizerRel] = List(
      OptimizerRelFactory.createOptimizerRel("a", "b"),
      OptimizerRelFactory.createOptimizerRel("b", "c"))
    val twoBagWithRootAB = new GHDNode(PATH2.take(1), Array())
    twoBagWithRootAB.children = List(new GHDNode(PATH2.tail.take(1), Array()))

    val listOfBags = twoBagWithRootAB.iterator.toList

    assertResult(Set("a", "b"))(listOfBags.head.attrSet)
    assertResult(Set("b", "c"))(listOfBags.tail.head.attrSet)
  }

  test("Check that iterator works correctly on a more complex GHD") {
    val TADPOLE: List[OptimizerRel] = List(
      OptimizerRelFactory.createOptimizerRel("a", "b"),
      OptimizerRelFactory.createOptimizerRel("b", "c"),
      OptimizerRelFactory.createOptimizerRel("c", "a"),
      OptimizerRelFactory.createOptimizerRel("a", "e"))
    val decomp1 = new GHDNode(List(TADPOLE(0)), Array())
    val decomp1Child1 = new GHDNode(List(TADPOLE(1), TADPOLE(2)), Array())
    val decomp1Child2 = new GHDNode(List(TADPOLE(3)), Array())
    decomp1.children = List(decomp1Child1, decomp1Child2)

    val listOfBags = decomp1.iterator.toList

    assertResult(Set("a", "b"))(listOfBags.head.attrSet)
    assertResult(Set("a", "b", "c"))(listOfBags(1).attrSet)
    assertResult(Set("a", "e"))(listOfBags(2).attrSet)
  }

  test("Test that we don't give weight to cover equality-selected attrs") {
    val justTriangle = List(
      OptimizerRelFactory.createOptimizerRel("a", "b"),
      OptimizerRelFactory.createOptimizerRel("b", "c"),
      OptimizerRelFactory.createOptimizerRel("a", "c"))
    val bag = new GHDNode(justTriangle, Array(Selection("d", EQUALS(), SelectionLiteral("0"))))
    assertResult(1.5)(bag.fractionalScoreTree())
  }
}