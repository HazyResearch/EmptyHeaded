package scala

import duncecap._
import org.scalatest.FunSuite

class QueryCompilerTest extends FunSuite {
  test("Test that query optimizer can return a single relation in 1 bag") {
    val result = Result(Rel("bag_0_a_b", Attributes(List("a", "b")), Annotations(List())))
    val operation = Operation("*")
    val order =  Order(Attributes(List("a", "b")))
    val project = Project(Attributes(List()))
    val join = Join(List(
      Rel("Edge", Attributes(List("a", "b")), Annotations(List()))))
    val agg = Aggregations(List())
    val filters = Filters(List())
    val rule = Rule(
      result,
      None,
      operation,
      order,
      project,
      join,
      agg,
      filters
    )
    val ir = IR(List(rule))

    val optimized = QueryCompiler.findOptimizedPlans("Simple(a,b) :- Edge(a,b).").head
    assertResult(result)(optimized.statements.head.result)
    assertResult(operation)(optimized.statements.head.operation)
    assertResult(order)(optimized.statements.head.order)
    assertResult(project)(optimized.statements.head.project)
    assertResult(join)(optimized.statements.head.join)
    assertResult(rule)(optimized.statements.head)
    assertResult(ir)(optimized)
  }

  test("Test that query optimizer can return triangle query in 1 bag") {
    val ir = IR(List(Rule(
      Result(Rel("bag_0_a_b_c", Attributes(List("a", "b", "c")), Annotations(List()))),
      None,
      Operation("*"),
      Order(Attributes(List("a", "b", "c"))),
      Project(Attributes(List())),
      Join(List(
        Rel("Edge", Attributes(List("a", "b")), Annotations(List())),
        Rel("Edge", Attributes(List("b", "c")), Annotations(List())),
        Rel("Edge", Attributes(List("a", "c")), Annotations(List())))),
      Aggregations(List()),
      Filters(List())
    )))

    val optimized = QueryCompiler.findOptimizedPlans("Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).").head
    assertResult(optimized)(ir)
  }

  test("lollipop query") {
    val bag1 = Rule(
      Result(Rel("bag_1_a_b_c", Attributes(List("a", "b", "c")), Annotations(List()))),
      None,
      Operation("*"),
      Order(Attributes(List("a", "b", "c"))),
      Project(Attributes(List())),
      Join(List(
        Rel("Edge", Attributes(List("a", "b")), Annotations(List())),
        Rel("Edge", Attributes(List("b", "c")), Annotations(List())),
        Rel("Edge", Attributes(List("a", "c")), Annotations(List())))),
      Aggregations(List()),
      Filters(List())
    )
    val bag0 = Rule(
      Result(Rel("bag_0_a_d", Attributes(List("a", "b", "c", "d")), Annotations(List()))),
      None,
      Operation("*"),
      Order(Attributes(List("a", "d", "b", "c"))),
      Project(Attributes(List())),
      Join(List(
        Rel("Edge", Attributes(List("a", "d")), Annotations(List())),
        Rel("bag_1_a_b_c", Attributes(List("a", "b", "c")), Annotations(List())))),
      Aggregations(List()),
      Filters(List())
    )
    val optimized = QueryCompiler.findOptimizedPlans("Lollipop(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d).").head
    assertResult(IR(List(bag0, bag1)))(optimized)
  }

  test("barbell query") {
    val bag2 = Rule(
      Result(Rel("bag_1_d_e_f", Attributes(List("d", "e", "f")), Annotations(List()))),
      None,
      Operation("*"),
      Order(Attributes(List("d", "e", "f"))),
      Project(Attributes(List())),
      Join(List(
        Rel("Edge", Attributes(List("d", "f")), Annotations(List())),
        Rel("Edge", Attributes(List("e", "f")), Annotations(List())),
        Rel("Edge", Attributes(List("d", "e")), Annotations(List()))
      )),
      Aggregations(List()),
      Filters(List())
    )

    val bag1 = Rule(
      Result(Rel("bag_1_a_b_c", Attributes(List("a", "b", "c")), Annotations(List()))),
      None,
      Operation("*"),
      Order(Attributes(List("a", "b", "c"))),
      Project(Attributes(List())),
      Join(List(
        Rel("Edge", Attributes(List("a", "c")), Annotations(List())),
        Rel("Edge", Attributes(List("b", "c")), Annotations(List())),
        Rel("Edge", Attributes(List("a", "b")), Annotations(List()))
      )),
      Aggregations(List()),
      Filters(List())
    )

    val bag0 = Rule(
      Result(Rel("bag_0_a_d", Attributes(List("a", "b", "c", "d", "e", "f")), Annotations(List()))),
      None,
      Operation("*"),
      Order(Attributes(List("a", "d", "e", "f", "b", "c"))),
      Project(Attributes(List())),
      Join(List(
        Rel("Edge", Attributes(List("a", "d")), Annotations(List())),
        Rel("bag_1_a_b_c", Attributes(List("a", "b", "c")), Annotations(List())),
        Rel("bag_1_d_e_f", Attributes(List("d", "e", "f")), Annotations(List())))),
      Aggregations(List()),
      Filters(List())
    )

    val optimized = QueryCompiler.findOptimizedPlans("Barbell(a,b,c,d,e,f) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(d,e),Edge(e,f),Edge(d,f),Edge(a,d).").head
    assertResult(IR(List(bag0, bag1, bag2)))(optimized)
  }

  test("aggregation triangle query") {
    val ir = IR(List(Rule(
      Result(Rel("bag_0_a_b_c", Attributes(List("a", "b", "c")), Annotations(List()))),
      None,
      Operation("*"),
      Order(Attributes(List("a", "b", "c"))),
      Project(Attributes(List())),
      Join(List(
        Rel("Edge", Attributes(List("a", "b")), Annotations(List())),
        Rel("Edge", Attributes(List("b", "c")), Annotations(List())),
        Rel("Edge", Attributes(List("a", "c")), Annotations(List())))),
      Aggregations(List()),
      Filters(List())
    )))

    val optimized = QueryCompiler.findOptimizedPlans("Triangle(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),z:uint64<-[COUNT(*)]").head
    println(optimized)
   // assertResult(optimized)(ir)
  }

  test("lollipop agg") {
    val optimized = QueryCompiler.findOptimizedPlans("Lollipop(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),z:uint64<-[COUNT(*)]").head
    println(optimized)
  }
}
