package duncecap

import org.scalatest.FunSuite

class IROptimizerTest extends FunSuite {

  test("Check that you can substitute correctly") {
    val rule = Rule(
      Result(Rel("Barbell_root", Attributes(List("a", "d")), Annotations(List())), true),
      None,
      Operation("*"),
      Order(Attributes(List("a", "d", "e", "f", "b", "c"))),
      Project(Attributes(List("b", "c", "e", "f"))),
      Join(List(
        Rel("Edge", Attributes(List("a", "d")), Annotations(List())),
        Rel("bag_1_a_b_c_Barbell", Attributes(List("a", "b", "c")), Annotations(List())),
        Rel("bag_1_d_e_f_Barbell", Attributes(List("d", "e", "f")), Annotations(List())))),
      Aggregations(List()),
      Filters(List())
    )
    val substitutions = Map() + ("bag_1_a_b_c_Barbell" -> "bag_1_d_e_f_Barbell")
    val withSub = IROptimizer.applySubstitutionsToRule(rule, substitutions)
    val expectedResult = Rule(
      Result(Rel("Barbell_root", Attributes(List("a", "d")), Annotations(List())), true),
      None,
      Operation("*"),
      Order(Attributes(List("a", "d", "e", "f", "b", "c"))),
      Project(Attributes(List("b", "c", "e", "f"))),
      Join(List(
        Rel("Edge", Attributes(List("a", "d")), Annotations(List())),
        Rel("bag_1_d_e_f_Barbell", Attributes(List("a", "b", "c")), Annotations(List())),
        Rel("bag_1_d_e_f_Barbell", Attributes(List("d", "e", "f")), Annotations(List())))),
      Aggregations(List()),
      Filters(List())
    )
    assertResult(expectedResult)(withSub)
  }

  test("Check that you can detect that two rules compute the same thing") {
    val bag2 = Rule(
      Result(Rel("bag_1_d_e_f_Barbell", Attributes(List("d", "e", "f")), Annotations(List())), true),
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
      Result(Rel("bag_1_a_b_c_Barbell", Attributes(List("a", "b", "c")), Annotations(List())), true),
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

    assert(bag1.attrNameAgnosticEquals(bag2))
  }

  test("test that you can identify when rules are different because of selections") {
    val bag2 = Rule(
      Result(Rel("bag_1_d_e_f_Barbell", Attributes(List("d", "e", "f")), Annotations(List())), true),
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
      Result(Rel("bag_1_a_b_c_Barbell", Attributes(List("a", "b", "c")), Annotations(List())), true),
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
      Filters(List(Selection("a",EQUALS(),"0")))
    )

    assert(!bag1.attrNameAgnosticEquals(bag2))
  }

  test("test that you can rewrite rules to dedup bags") {
    val bag2 = Rule(
      Result(Rel("bag_1_d_e_f_Barbell", Attributes(List("d", "e", "f")), Annotations(List())), true),
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

    val bag0 = Rule(
      Result(Rel("Barbell_root", Attributes(List("a", "d")), Annotations(List())), true),
      None,
      Operation("*"),
      Order(Attributes(List("a", "d", "e", "f", "b", "c"))),
      Project(Attributes(List("b", "c", "e", "f"))),
      Join(List(
        Rel("Edge", Attributes(List("a", "d")), Annotations(List())),
        Rel("bag_1_d_e_f_Barbell", Attributes(List("a", "b", "c")), Annotations(List())),
        Rel("bag_1_d_e_f_Barbell", Attributes(List("d", "e", "f")), Annotations(List())))),
      Aggregations(List()),
      Filters(List())
    )

    val topdownPass = Rule(
      Result(Rel("Barbell", Attributes(List("a", "b", "c", "d", "e","f")), Annotations(List())), false),
      None,
      Operation("*"),
      Order(Attributes(List("a", "d", "e", "f", "b", "c"))),
      Project(Attributes(List())),
      Join(List(
        Rel("Barbell_root", Attributes(List("a", "d")), Annotations(List())),
        Rel("bag_1_d_e_f_Barbell", Attributes(List("a", "b", "c")), Annotations(List())),
        Rel("bag_1_d_e_f_Barbell", Attributes(List("d", "e", "f")), Annotations(List()))
      )),
      Aggregations(List()),
      Filters(List())
    )

    val plan = QueryPlanner.findOptimizedPlans(DatalogParser.run(
      "Barbell(a,b,c,d,e,f) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(d,e),Edge(e,f),Edge(d,f),Edge(a,d)."))
    val optimized = IR(IROptimizer.dedupComputationsHelper(plan.rules, List(), Map()))
    assertResult(IR(List(bag2, bag0, topdownPass)))(optimized)
  }

}
