package solver

import DunceCap.attr._
import DunceCap.{AttrOrderingUtil, GHDNode, QueryRelationFactory}
import org.scalatest.FunSuite

class AttributeOrderingTest extends FunSuite {
  test("Can reorder attributes so that equality selected ones are at the front") {
    val partitioned = AttrOrderingUtil.partition_equality_selected(
      List[Attr]("a","b","x","c", "y", "z"),
      List[(Attr, SelectionOp, SelectionVal)](
        ("a", "", ""),
        ("y", "=", "1"),
        ("b", "", ""),
        ("x", "=", "1"),
        ("c", "", ""),
        ("z", "<", "100"),
        ("z", "=", "5")
      ))
    assertResult(Set[Attr]("x", "y", "z"))(partitioned.take(3).toSet)
  }

  test("Can order attributes, taking into account GHD structure and what's equality-selected") {
    val rootBag = new GHDNode(List(
      QueryRelationFactory.createQueryRelationWithNoSelects(List[Attr]("a", "b")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List[Attr]("b", "c")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List[Attr]("c", "a"))))
    val child1 = new GHDNode(List(
      QueryRelationFactory.createQueryRelationWithEqualitySelect(List[Attr]("a"), List[Attr]("x"))))
    val child2 = new GHDNode(List(
      QueryRelationFactory.createQueryRelationWithEqualitySelect(List[Attr]("b"), List[Attr]("y"))))
    val child3 = new GHDNode(List(
      QueryRelationFactory.createQueryRelationWithEqualitySelect(List[Attr]("c"), List[Attr]("z"))))
    rootBag.children = List(child1, child2, child3)

    val ordering = AttrOrderingUtil.getAttributeOrdering(
      rootBag,
      rootBag.rels:::child1.rels:::child2.rels:::child3.rels,
      QueryRelationFactory.createQueryRelationWithNoSelects(List[Attr]("a", "b", "c")))

    assertResult(List[Attr]("x", "z", "y", "a", "b", "c"))(ordering)
  }
}
