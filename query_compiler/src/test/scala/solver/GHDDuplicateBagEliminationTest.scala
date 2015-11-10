package DunceCap

import DunceCap.attr.{AttrInfo, Attr}
import org.scalatest.FunSuite

class GHDDuplicateBagEliminationTest extends FunSuite {
  final val BARBELL: List[QueryRelation] = List(
    QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "b")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("b", "c")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "c")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "e")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("e", "f")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "f")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("b", "e"))
  )

  test("attrNameAgnosticRelationEquals returns mapping of attrs that would have to be true if these two rels are going to considered equal") {
    val result = GHD.attrNameAgnosticRelationEquals(
      QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "b")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "f")),
      Map[AttrInfo, AttrInfo](),
      Map[String, ParsedAggregate]())

    assert(result.isDefined)
    assertResult(Map[AttrInfo, AttrInfo](("a","","") -> ("d","",""), ("b","","") -> ("f","","")))(result.get)
  }

  test("attrNameAgnosticRelationEquals can map attrs to each other if they have same selections & aggs") {
    val result = GHD.attrNameAgnosticRelationEquals(
      new QueryRelation("R", List[AttrInfo](
        ("a", "=", "2"),
        ("b", "", ""))),
      new QueryRelation("R", List[AttrInfo](
        ("d", "=", "2"),
        ("f", "", ""))),
      Map[AttrInfo, AttrInfo](),
      Map[String, ParsedAggregate](
        "b" -> ParsedAggregate("+", "COUNT", "1"),
        "f" -> ParsedAggregate("+", "COUNT", "1")
      ))

    assert(result.isDefined)
    assertResult(Map[AttrInfo, AttrInfo](("a","=","2") -> ("d","=","2"), ("b","","") -> ("f","","")))(result.get)
  }

  test("attrNameAgnosticRelationEquals doesn't map attrs to each other if they have different selections") {
    val result = GHD.attrNameAgnosticRelationEquals(
      new QueryRelation("R", List[AttrInfo](
        ("a", "=", "2"),
        ("b", "<", "2"))),
      new QueryRelation("R", List[AttrInfo](
        ("d", "=", "2"),
        ("f", "=", "3"))),
      Map[AttrInfo, AttrInfo](),
      Map[String, ParsedAggregate]())

    assert(result.isEmpty)
  }

  test("attrNameAgnosticRelationEquals doesn't map attrs to each other if they have different aggregations") {
    val result = GHD.attrNameAgnosticRelationEquals(
      QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "b")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "f")),
      Map[AttrInfo, AttrInfo](),
      Map[String, ParsedAggregate](
        "a" -> ParsedAggregate("+", "COUNT", "1"),
        "f" -> ParsedAggregate("+", "COUNT", "1")
      ))

    assert(result.isEmpty)
  }

  test("attrNameAgnosticEquals detects that these two bags are the same") {
    val triangle1 = new GHDNode(BARBELL.take(3).reverse) // reverse just to check that this still works given a weird ordering
    val triangle2 = new GHDNode(BARBELL.drop(3).take(3))
    assert(triangle1.attrNameAgnosticEquals(triangle2, Map[String, ParsedAggregate]()))
    assert(triangle2.attrNameAgnosticEquals(triangle1, Map[String, ParsedAggregate]()))
  }

  test("Can eliminate duplicate bags correctly in barbell query") {
    val rootNodes = GHDSolver.getMinFHWDecompositions(BARBELL);
    val agg = ParsedAggregate("+", "COUNT", "1")
    val candidates = rootNodes.map(r => new GHD(
      r,
      BARBELL,
      Map[String,ParsedAggregate](("a" -> agg), ("c" -> agg), ("d" -> agg), ("f" -> agg)),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("b", "e"))));
    candidates.map(c => c.doPostProcessingPass())
    val chosen = HeuristicUtils.getGHDsWithMaxCoveringRoot(
      HeuristicUtils.getGHDsWithMinBags(candidates))
    val secondTriangleBag = chosen.head.getQueryPlan.ghd.last

    assert(secondTriangleBag.duplicateOf.isDefined)
    assertResult("bag_1_0")(secondTriangleBag.duplicateOf.get)
  }
}
