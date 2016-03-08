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
    val optimized = IROptimizer.dedupComputations(plan)
    assertResult(IR(List(bag2, bag0, topdownPass)))(optimized)
  }

  test("test that you can dedup rules that user inputs") {
   val expectedDeduped = IR(List(
     Rule(Result(Rel("Simple1",Attributes(List("a", "b")),Annotations(List())),false),
       None,
       Operation("*"),
       Order(Attributes(List("b", "a"))),
       Project(Attributes(List())),
       Join(List(Rel("Edge",Attributes(List("a", "b")),Annotations(List())))),Aggregations(List()),
       Filters(List(Selection("b",EQUALS(),"0")))),
     Rule(Result(Rel("Simple2",Attributes(List("b", "c")),Annotations(List())),false),
       None,
       Operation("*"),
       Order(Attributes(List("b", "c"))),
       Project(Attributes(List())),
       Join(List(Rel("Simple1",Attributes(List("a", "b")),Annotations(List())))),
       Aggregations(List()),
       Filters(List()))))

    val query =
      """
        |Simple1(a,b) :- Edge(a,b),b=0.
        |Simple2(b,c) :- Edge(b,c),c=0.
      """.stripMargin
    val plan = QueryPlanner.findOptimizedPlans(
      DatalogParser.run(query))
    val deduped = IROptimizer.dedupComputations(plan)

    assertResult(2)(deduped.rules.size) // make sure we don't get rid of any of them, since neither is an intermediate
    assertResult(expectedDeduped)(deduped)
  }


  test("test that you can dedup aggregated barbell properly") {
    val expectedDeduped = IR(List(
      Rule(Result(Rel("bag_1_x_y_z_BarbellAgg",Attributes(List("x")),Annotations(List("w"))),true),
        None,
        Operation("*"),
        Order(Attributes(List("x", "y", "z"))),
        Project(Attributes(List())),
        Join(List(
          Rel("Edge",Attributes(List("y", "z")), Annotations(List())), Rel("Edge",Attributes(List("x", "z")), Annotations(List())),
          Rel("Edge",Attributes(List("x", "y")),Annotations(List())))),
        Aggregations(List(Aggregation("w","long",SUM(),Attributes(List("y", "z")),"1","AGG",List()))),
        Filters(List())),
      Rule(Result(Rel("BarbellAgg",Attributes(List()),Annotations(List("w"))),false),
        None,
        Operation("*"),Order(Attributes(List("a", "x"))),Project(Attributes(List())),
        Join(List(
          Rel("Edge",Attributes(List("a", "x")),Annotations(List())),
          Rel("bag_1_x_y_z_BarbellAgg",Attributes(List("a")), Annotations(List("w"))),
          Rel("bag_1_x_y_z_BarbellAgg",Attributes(List("x")),Annotations(List("w"))))),
        Aggregations(List(Aggregation("w","long",SUM(),Attributes(List("a", "x")),"1","AGG",List()))),
        Filters(List()))))
    val query =
      """
        |BarbellAgg(;w) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),
        |Edge(x,y),Edge(y,z),Edge(x,z),w:long<-[COUNT(*)].""".stripMargin
    val plan = QueryPlanner.findOptimizedPlans(
      DatalogParser.run(query))
    val deduped = IROptimizer.dedupComputations(plan)
    assertResult(expectedDeduped)(deduped)
  }
}
