package duncecap

import org.scalatest.FunSuite

class EHGeneratorTest extends FunSuite {
  private val aggregation = Some(QueryPlanAggregation(
    "+", "1", "", None, None))

  private def accessor(relation:String, attr:String, annotated:Boolean = false) =
    QueryPlanAccessor(relation, Attributes(List(attr)), annotated)

  private def attr(
    name:String,
    accessors:List[QueryPlanAccessor],
    annotatedSelection:List[QueryPlanSelection] = List()) =
    QueryPlanAttrInfo(
      name,
      accessors,
      materialize = false,
      selection = annotatedSelection,
      annotation = None,
      aggregation = aggregation,
      prevMaterialized = None,
      nextMaterialized = None)

  private def iteratorAccessors(names:String*) = names.map(name =>
    name -> List(
      ("Left_" + name, 0, 1),
      ("Right_" + name, 0, 1))).toMap

  test("terminal unannotated aggregate counts without materializing") {
    val terminal = attr("x", List(
      accessor("Left", "x"),
      accessor("Right", "x")))

    val code = EHGenerator.nprrRecursiveCall(
      Some(terminal), List(), iteratorAccessors("x"), "long", List()).toString

    assert(code.contains("count_x = Builder->count_set("))
    assert(!code.contains("build_aggregated_set"))
    assert(!code.contains("foreach_aggregate"))
  }

  test("terminal aggregate with annotated input remains materialized and enumerated") {
    val terminal = attr("x", List(
      accessor("Left", "x", annotated = true),
      accessor("Right", "x")))

    val code = EHGenerator.nprrRecursiveCall(
      Some(terminal), List(), iteratorAccessors("x"), "long", List()).toString

    assert(code.contains("count_x = Builder->build_aggregated_set("))
    assert(code.contains("Builder->foreach_aggregate("))
    assert(!code.contains("Builder->count_set("))
  }

  test("nonterminal unannotated aggregate remains materialized for recursive foreach") {
    val outer = attr("x", List(
      accessor("Left", "x"),
      accessor("Right", "x")))
    val terminal = attr("y", List(
      accessor("Left", "y"),
      accessor("Right", "y")))

    val code = EHGenerator.nprrRecursiveCall(
      Some(outer), List(terminal), iteratorAccessors("x", "y"), "long", List()).toString

    assert(code.contains("count_x = Builder->build_aggregated_set("))
    assert(code.contains("Builder->foreach_aggregate("))
    assert(code.contains("count_y = Builder->count_set("))
  }

  test("terminal aggregate with a remaining selection stays materialized") {
    val terminal = attr(
      "x",
      List(accessor("Left", "x"), accessor("Right", "x")),
      List(QueryPlanSelection("=", "7")))

    assert(!EHGenerator.terminalCardinalityOnly(terminal,List()))
    val code = EHGenerator.nprrRecursiveCall(
      Some(terminal), List(), iteratorAccessors("x"), "long", List()).toString
    assert(code.contains("count_x = Builder->build_aggregated_set("))
  }

  test("terminal three-way intersection stays materialized without an n-way count kernel") {
    val terminal = attr("x", List(
      accessor("Left", "x"),
      accessor("Middle", "x"),
      accessor("Right", "x")))
    val iterators = Map("x" -> List(
      ("Left_x", 0, 1),
      ("Middle_x", 0, 1),
      ("Right_x", 0, 1)))

    assert(!EHGenerator.terminalCardinalityOnly(terminal,List()))
    val code = EHGenerator.nprrRecursiveCall(
      Some(terminal), List(), iterators, "long", List()).toString
    assert(code.contains("count_x = Builder->build_aggregated_set(&x_sets)"))
    assert(!code.contains("Builder->count_set("))
  }
}
