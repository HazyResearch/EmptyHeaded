package scala.solver

import DunceCap.{GHDNode, QueryRelationFactory, QueryRelation}
import org.scalatest.FunSuite

/**
 * Created by sctu on 1/5/16.
 */
class GHDNodeTest extends FunSuite {
  test("Check that iterator works correctly on a two node GHD") {
    val PATH2: List[QueryRelation] = List(
      QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "b")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("b", "c")))
    val twoBagWithRootAB = new GHDNode(PATH2.take(1))
    twoBagWithRootAB.children = List(new GHDNode(PATH2.tail.take(1)))

    val listOfBags = twoBagWithRootAB.iterator.toList

    assertResult(Set("a", "b"))(listOfBags.head.attrSet)
    assertResult(Set("b", "c"))(listOfBags.tail.head.attrSet)
  }

  test("Check that iterator works correctly on a more complex GHD") {
    val TADPOLE: List[QueryRelation] = List(
      QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "b")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("b", "c")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("c", "a")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "e")))
    val decomp1 = new GHDNode(List(TADPOLE(0)))
    val decomp1Child1 = new GHDNode(List(TADPOLE(1), TADPOLE(2)))
    val decomp1Child2 = new GHDNode(List(TADPOLE(3)))
    decomp1.children = List(decomp1Child1, decomp1Child2)

    val listOfBags = decomp1.iterator.toList
    println(decomp1)

    assertResult(Set("a", "b"))(listOfBags.head.attrSet)
    assertResult(Set("a", "b", "c"))(listOfBags(1).attrSet)
    assertResult(Set("a", "e"))(listOfBags(2).attrSet)
  }
}
