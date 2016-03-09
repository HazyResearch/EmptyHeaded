package duncecap

import duncecap.attr.Attr
import org.scalatest.FunSuite

class AttrOrderingUtilTest extends FunSuite {
  test("Can reorder attributes so that equality selected ones are at the front") {
    val partitioned = AttrOrderingUtil.partition_equality_selected(
      List("a","b","x","c", "y", "z"),
      List(
        Selection("y", EQUALS(), "1"),
        Selection("x", EQUALS(), "1"),
        Selection("z", EQUALS(), "100"),
        Selection("z", EQUALS(), "5")
      ))
    assertResult(Set[Attr]("x", "y", "z"))(partitioned.take(3).toSet)
  }

  test("Can order attributes, taking into account GHD structure and what's equality-selected") {
    val rootBag = new GHDNode(List(
      OptimizerRelFactory.createOptimizerRelWithNoSelects("a", "b"),
      OptimizerRelFactory.createOptimizerRelWithNoSelects("b", "c"),
      OptimizerRelFactory.createOptimizerRelWithNoSelects("c", "a")), Array())
    val child1 = new GHDNode(List(
      OptimizerRelFactory.createOptimizerRelWithNoSelects("a", "x")), Array(Selection("x", EQUALS(), "0")))
    val child2 = new GHDNode(List(
      OptimizerRelFactory.createOptimizerRelWithNoSelects("b","y")), Array(Selection("y", EQUALS(), "0")))
    val child3 = new GHDNode(List(
      OptimizerRelFactory.createOptimizerRelWithNoSelects("c","z")),  Array(Selection("z", EQUALS(), "0")))
    rootBag.children = List(child1, child2, child3)

    val ordering = AttrOrderingUtil.getAttributeOrdering(
      rootBag,
      rootBag.rels:::child1.rels:::child2.rels:::child3.rels,
      OptimizerRelFactory.createOptimizerRelWithNoSelects("a", "b", "c"),
      List(Selection("x", EQUALS(), "0"),Selection("y", EQUALS(), "0"), Selection("z", EQUALS(), "0")))

    assertResult(Set[Attr]("x", "y", "z"))(ordering.take(3).toSet)
  }
}
