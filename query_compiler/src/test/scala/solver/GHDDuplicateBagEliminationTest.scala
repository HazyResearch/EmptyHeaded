package DunceCap

import DunceCap.attr.Attr
import org.scalatest.FunSuite

class GHDDuplicateBagEliminationTest extends FunSuite {
  final val BARBELL: List[QueryRelation] = List(
    QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "b")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("b", "c")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "c")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "e")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("e", "f")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "f")),
    QueryRelationFactory.createQueryRelationWithNoSelects(List("c", "d"))
  )

  test("attrNameAgnosticRelationEquals returns mapping of attrs that would have to be true if these two rels are going to considered equal") {
    val result = GHD.attrNameAgnosticRelationEquals(
      QueryRelationFactory.createQueryRelationWithNoSelects(List("a", "b")),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("d", "f")),
      Map[Attr, Attr]())

    assert(result.isDefined)
    assertResult(Map[Attr, Attr]("a" -> "d", "b" -> "f"))(result.get)
  }

  test("attrNameAgnosticEquals detects that these two bags are the same") {
    val triangle1 = new GHDNode(BARBELL.take(3))
    val triangle2 = new GHDNode(BARBELL.drop(3).take(3))
    assert(triangle1.attrNameAgnosticEquals(triangle2))
    assert(triangle2.attrNameAgnosticEquals(triangle1))
  }

  test("Can eliminate duplicate bags correctly in barbell query") {
    val rootNodes = GHDSolver.getMinFHWDecompositions(BARBELL);
    val agg = ParsedAggregate("+", "COUNT", "1")
    val candidates = rootNodes.map(r => new GHD(
      r,
      BARBELL,
      Map[String,ParsedAggregate](("a" -> agg), ("b" -> agg), ("e" -> agg), ("f" -> agg)),
      QueryRelationFactory.createQueryRelationWithNoSelects(List("c", "d"))));
    candidates.map(c => c.doPostProcessingPass())
    val chosen = HeuristicUtils.getGHDsWithMaxCoveringRoot(
      HeuristicUtils.getGHDsWithMinBags(candidates))
    println(chosen.head.getQueryPlan)
  }
}
