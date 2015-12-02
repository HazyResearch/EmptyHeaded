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

  final val UNDIRECTED_BARBELL: List[QueryRelation] = List(
    QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "b")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("b", "c")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "c")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "e")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("e", "f")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "f")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("b", "f")),
    // now for all the edges in the opposite direction
    QueryRelationFactory.createQueryRelationWithNoSelects(List("b", "a")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("c", "b")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("c", "a")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("e", "d")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("f", "e")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("f", "d")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("f", "b"))
  )

  test("attrNameAgnosticRelationEquals returns mapping of attrs that would have to be true if these two rels are going to considered equal") {
    val result = PlanUtil.attrNameAgnosticRelationEquals(
      QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "b")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "b")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "f")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "f")),
      Map[Attr, Attr](),
      Map[String, ParsedAggregate]())

    assert(result.isDefined)
    assertResult(Map[Attr, Attr]("a"-> "d", "b" -> "f"))(result.get)
  }

  test("attrNameAgnosticRelationEquals can map attrs to each other if they have same selections & aggs") {
    val result = PlanUtil.attrNameAgnosticRelationEquals(
      QueryRelationFactory.createQueryRelationWithNoSelects(List("a")),
      new QueryRelation("R", List[AttrInfo](
        ("a", "=", "2"),
        ("b", "", ""))),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("d")),
      new QueryRelation("R", List[AttrInfo](
        ("d", "=", "2"),
        ("f", "", ""))),
      Map[Attr, Attr](),
      Map[String, ParsedAggregate](
        "b" -> ParsedAggregate("+", "COUNT", "1"),
        "f" -> ParsedAggregate("+", "COUNT", "1")
      ))

    assert(result.isDefined)
    assertResult(Map[Attr, Attr]("a" -> "d", "b" -> "f"))(result.get)
  }

  test("attrNameAgnosticRelationEquals doesn't map attrs to each other if they have different selections") {
    val result = PlanUtil.attrNameAgnosticRelationEquals(
      QueryRelationFactory.createQueryRelationWithNoSelects(List("a")),
      new QueryRelation("R", List[AttrInfo](
        ("a", "=", "2"),
        ("b", "<", "2"))),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("d")),
      new QueryRelation("R", List[AttrInfo](
        ("d", "=", "2"),
        ("f", "=", "3"))),
      Map[Attr, Attr](),
      Map[String, ParsedAggregate]())

    assert(result.isEmpty)
  }

  test("attrNameAgnosticRelationEquals doesn't map attrs to each other if they have different aggregations") {
    val result = PlanUtil.attrNameAgnosticRelationEquals(
      QueryRelationFactory.createQueryRelationWithNoSelects(List("b")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "b")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("d")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "f")),
      Map[Attr, Attr](),
      Map[String, ParsedAggregate](
        "a" -> ParsedAggregate("+", "COUNT", "1"),
        "f" -> ParsedAggregate("+", "COUNT", "1")
      ))

    assert(result.isEmpty)
  }

  test("attrNameAgnosticEquals detects that these two bags are the same") {
    val triangle1 = new GHDNode(BARBELL.take(3).reverse) // reverse just to check that this still works given a weird ordering
    triangle1.computeProjectedOutAttrsAndOutputRelation("int", Set[Attr]("b"), Set())
    val triangle2 = new GHDNode(BARBELL.drop(3).take(3))
    triangle2.computeProjectedOutAttrsAndOutputRelation("int", Set[Attr]("e"), Set())
    assert(triangle1.attrNameAgnosticEquals(triangle2, Map[String, ParsedAggregate]()))
    assert(triangle2.attrNameAgnosticEquals(triangle1, Map[String, ParsedAggregate]()))
  }

  test("Can eliminate duplicate bags correctly in directed barbell query") {
    val rootNodes = GHDSolver.getMinFHWDecompositions(BARBELL);
    val agg = ParsedAggregate("+", "COUNT", "1")
    val candidates = rootNodes.map(r => new GHD(
      r,
      BARBELL,
      Map[String,ParsedAggregate](("a" -> agg), ("c" -> agg), ("d" -> agg), ("f" -> agg)),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("b", "e"))))
    candidates.map(c => c.doPostProcessingPass())
    val chosen = HeuristicUtils.getGHDsWithMaxCoveringRoot(
      HeuristicUtils.getGHDsWithMinBags(candidates))
    chosen.head.doBagDedup
    val secondTriangleBag = chosen.head.getQueryPlan.ghd(1)

    assert(secondTriangleBag.duplicateOf.isDefined)
    assertResult(chosen.head.getQueryPlan.ghd(0).name)(secondTriangleBag.duplicateOf.get)
  } 

  test("Can eliminate duplicate bags correctly in undirected barbell query") {
    //println(Calendar.getInstance().getTime())
    val rootNodes = GHDSolver.getMinFHWDecompositions(UNDIRECTED_BARBELL);
    val agg = ParsedAggregate("+", "COUNT", "1")
    //println(Calendar.getInstance().getTime())
    val candidates = rootNodes.map(r => new GHD(
      r,
      UNDIRECTED_BARBELL,
      Map[String,ParsedAggregate](("a" -> agg), ("c" -> agg), ("d" -> agg), ("e" -> agg)),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("b", "f"))));
    candidates.map(c => c.doPostProcessingPass())
    val chosen = HeuristicUtils.getGHDsWithMaxCoveringRoot(
      HeuristicUtils.getGHDsWithMinBags(candidates))
    chosen.head.doBagDedup
    val secondTriangleBag = chosen.head.getQueryPlan.ghd(1)

   assert(secondTriangleBag.duplicateOf.isDefined)
   assertResult(chosen.head.getQueryPlan.ghd(0).name)(secondTriangleBag.duplicateOf.get)
  }

  test("more than 2 attrs test case, with aggregations, projections, selects") {
    val child1 = new GHDNode(List[QueryRelation](
      new QueryRelation("R", List[AttrInfo](
        ("a", "", ""),
        ("b", "<", "2"))),
      new QueryRelation("S", List[AttrInfo](
        ("a", "=", "2"),
        ("c", "", ""),
        ("d", "", ""))),
      new QueryRelation("R", List[AttrInfo](
        ("d", "", ""),
        ("e", "", "")))))
    val child2 = new GHDNode(List[QueryRelation](
      new QueryRelation("R", List[AttrInfo](
        ("x", "", ""),
        ("y", "<", "2"))),
      new QueryRelation("R", List[AttrInfo](
        ("z", "", ""),
        ("n", "", ""))),
      new QueryRelation("S", List[AttrInfo](
        ("x", "=", "2"),
        ("m", "", ""),
        ("z", "", "")))))
    val agg = ParsedAggregate("+", "COUNT", "1")
    val joinAggs =  Map[String, ParsedAggregate]("e" -> agg, "n" -> agg)

    val root = new GHDNode(List[QueryRelation](QueryRelationFactory.createQueryRelationWithNoSelects(List[String]("b", "y"))))
    root.children = List[GHDNode](child1, child2)
    val ghd = new GHD(
      root,
      child1.rels:::child2.rels:::root.rels,
      joinAggs,
      QueryRelationFactory.createQueryRelationWithNoSelects(List("y", "b")))
    ghd.doPostProcessingPass
    ghd.doBagDedup

    assert(ghd.getQueryPlan.ghd(1).duplicateOf.isDefined)
    assertResult("bag_1_a_b_c_d_e")(ghd.getQueryPlan.ghd(1).duplicateOf.get)
  }

  test("more than 2 attrs negative test case, with aggregations, projections, selects") {
    val child1 = new GHDNode(List[QueryRelation](
      new QueryRelation("R", List[AttrInfo](
        ("a", "", ""),
        ("b", "<", "2"))),
      new QueryRelation("S", List[AttrInfo](
        ("a", "=", "2"),
        ("c", "", ""),
        ("d", "", ""))),
      new QueryRelation("R", List[AttrInfo](
        ("d", "", ""),
        ("e", "", "")))))
    val child2 = new GHDNode(List[QueryRelation](
      new QueryRelation("R", List[AttrInfo](
        ("x", "", ""),
        ("y", "<", "2"))),
      new QueryRelation("R", List[AttrInfo](
        ("z", "", ""),
        ("n", "", ""))),
      new QueryRelation("S", List[AttrInfo](
        ("k", "=", "2"),
        ("m", "", ""),
        ("z", "", "")))))
    val agg = ParsedAggregate("+", "COUNT", "1")
    val joinAggs =  Map[String, ParsedAggregate]("e" -> agg, "n" -> agg)
    child1.computeProjectedOutAttrsAndOutputRelation("int", Set[Attr]("b", "y"), Set())
    child2.computeProjectedOutAttrsAndOutputRelation("int", Set[Attr]("b", "y"), Set())

    assert(!child1.attrNameAgnosticEquals(child2, joinAggs))
    assert(!child2.attrNameAgnosticEquals(child1, joinAggs))
  }
}
