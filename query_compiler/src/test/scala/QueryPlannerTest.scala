package duncecap

import org.scalatest.FunSuite

class QueryPlannerTest extends FunSuite {
  test("Test that query optimizer can return a single relation in 1 bag") {
    val result = Result(Rel("Simple", Attributes(List("a", "b")), Annotations(List())), false)
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

    val optimized = IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(
      DatalogParser.run("Simple(a,b) :- Edge(a,b).")))
    assertResult(result)(optimized.rules.head.result)
    assertResult(operation)(optimized.rules.head.operation)
    assertResult(order)(optimized.rules.head.order)
    assertResult(project)(optimized.rules.head.project)
    assertResult(join)(optimized.rules.head.join)
    assertResult(rule)(optimized.rules.head)
    assertResult(ir)(optimized)
  }

  test("Test that query optimizer can return triangle query in 1 bag") {
    val ir = IR(List(Rule(
      Result(Rel("Triangle", Attributes(List("a", "b", "c")), Annotations(List())), false),
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

    val optimized = QueryPlanner.findOptimizedPlans(DatalogParser.run("Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c)."))
    assertResult(optimized)(ir)
  }

  test("Test that query optimizer can return a projected triangle query in 1 bag") {
    val ir = IR(List(Rule(
      Result(Rel("Triangle", Attributes(List("a", "b")), Annotations(List())), false),
      None,
      Operation("*"),
      Order(Attributes(List("a", "b", "c"))),
      Project(Attributes(List("c"))),
      Join(List(
        Rel("Edge", Attributes(List("a", "b")), Annotations(List())),
        Rel("Edge", Attributes(List("b", "c")), Annotations(List())),
        Rel("Edge", Attributes(List("a", "c")), Annotations(List())))),
      Aggregations(List()),
      Filters(List())
    )))

    val optimized = QueryPlanner.findOptimizedPlans(DatalogParser.run("Triangle(a,b) :- Edge(a,b),Edge(b,c),Edge(a,c)."))
    assertResult(optimized)(ir)
  }

  test("Scratch") {
    val optimized = QueryPlanner.findOptimizedPlans(DatalogParser.run("Triangle(a;w) :- Edge(a,b),Edge(b,c),w:float<-[SUM(b)]."))
  }

  test("lollipop query") {
    val bag1 = Rule(
      Result(Rel("bag_1_a_b_c_Lollipop", Attributes(List("a", "b", "c")), Annotations(List())), true),
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
      Result(Rel("Lollipop_root", Attributes(List("a", "d")), Annotations(List())), true),
      None,
      Operation("*"),
      Order(Attributes(List("a", "d", "b", "c"))),
      Project(Attributes(List("b", "c"))),
      Join(List(
        Rel("Edge", Attributes(List("a", "d")), Annotations(List())),
        Rel("bag_1_a_b_c_Lollipop", Attributes(List("a", "b", "c")), Annotations(List())))),
      Aggregations(List()),
      Filters(List())
    )

    val topdownPass = Rule(
      Result(Rel("Lollipop", Attributes(List("a", "b", "c", "d")), Annotations(List())), false),
      None,
      Operation("*"),
      Order(Attributes(List("a", "d", "b", "c"))),
      Project(Attributes(List())),
      Join(List(
        Rel("Lollipop_root", Attributes(List("a", "d")), Annotations(List())),
        Rel("bag_1_a_b_c_Lollipop", Attributes(List("a", "b", "c")), Annotations(List()))
      )),
      Aggregations(List()),
      Filters(List())
    )

    val optimized = IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(DatalogParser.run("Lollipop(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d).")))
    assertResult(IR(List(bag1, bag0, topdownPass)))(optimized)
  }

  test("barbell query") {
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

    val bag0 = Rule(
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

    val topdownPass = Rule(
      Result(Rel("Barbell", Attributes(List("a", "b", "c", "d", "e","f")), Annotations(List())), false),
      None,
      Operation("*"),
      Order(Attributes(List("a", "d", "e", "f", "b", "c"))),
      Project(Attributes(List())),
      Join(List(
        Rel("Barbell_root", Attributes(List("a", "d")), Annotations(List())),
        Rel("bag_1_a_b_c_Barbell", Attributes(List("a", "b", "c")), Annotations(List())),
        Rel("bag_1_d_e_f_Barbell", Attributes(List("d", "e", "f")), Annotations(List()))
      )),
      Aggregations(List()),
      Filters(List())
    )

    val optimized = QueryPlanner.findOptimizedPlans(DatalogParser.run(
      "Barbell(a,b,c,d,e,f) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(d,e),Edge(e,f),Edge(d,f),Edge(a,d)."))
    assertResult(topdownPass)(optimized.rules(3))
    assertResult(bag0)(optimized.rules(2))
    assertResult(bag1)(optimized.rules(1))
    assertResult(bag2)(optimized.rules(0))
    assertResult(IR(List(bag2, bag1, bag0, topdownPass)))(optimized)
  }

  test("lollipop agg") {
    val ir = IR(List(
      Rule(Result(
        Rel("bag_1_a_b_c_Lollipop",Attributes(List("a")),Annotations(List("z"))), true),None,Operation("*"),Order(Attributes(List("a", "b", "c"))),Project(Attributes(List())),
        Join(List(
          Rel("Edge",Attributes(List("a", "c")),Annotations(List())),
          Rel("Edge",Attributes(List("b", "c")),Annotations(List())),
          Rel("Edge",Attributes(List("a", "b")),Annotations(List())))),
        Aggregations(List(Aggregation("z","long",SUM(),Attributes(List("b", "c")),"1","AGG", List()))),Filters(List())),
      Rule(Result(
        Rel("Lollipop",Attributes(List()),Annotations(List("z"))), false),None,Operation("*"),Order(Attributes(List("a", "d"))),Project(Attributes(List())),
        Join(List(
          Rel("Edge",Attributes(List("a", "d")),Annotations(List())),
          Rel("bag_1_a_b_c_Lollipop",Attributes(List("a")),Annotations(List("z"))))),
        Aggregations(List(Aggregation("z","long",SUM(),Attributes(List("a", "d")),"1","AGG", List()))),Filters(List()))
      ))
    val optimized = IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(DatalogParser.run("Lollipop(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),z:long<-[COUNT(*)]")))
    assertResult(ir)(optimized)
  }

  test("4 clique with selection and aggregation") {
    val ir = IR(List(
      Rule(Result(
        Rel("bag_1_x_a_FliqueSelAgg",Attributes(List("a")),Annotations(List("z"))),true),
        None,
        Operation("*"),
        Order(Attributes(List("x", "a"))),
        Project(Attributes(List())),
        Join(List(Rel("Edge",Attributes(List("a", "x")),Annotations(List())))),
        Aggregations(List(Aggregation("z","long",SUM(),Attributes(List("x")),"1","AGG", List()))),Filters(List(Selection("x",EQUALS(),"0")))),
      Rule(Result(
        Rel("FliqueSelAgg",Attributes(List()),Annotations(List("z"))),false),
        None,
        Operation("*"),
        Order(Attributes(List("a", "b", "c", "d"))),
        Project(Attributes(List())),
        Join(List(
          Rel("Edge",Attributes(List("c", "d")),Annotations(List())),
          Rel("Edge",Attributes(List("b", "c")),Annotations(List())),
          Rel("Edge",Attributes(List("b", "d")),Annotations(List())),
          Rel("Edge",Attributes(List("a", "c")),Annotations(List())),
          Rel("bag_1_x_a_FliqueSelAgg",Attributes(List("a")),Annotations(List("z"))),
          Rel("Edge",Attributes(List("a", "d")),Annotations(List())),
          Rel("Edge",Attributes(List("a", "b")),Annotations(List())))),
        Aggregations(List(Aggregation("z","long",SUM(),Attributes(List("a", "b", "c", "d")),"1","AGG", List()))),Filters(List()))
      ))
    val optimized = IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(DatalogParser.run(
      "FliqueSelAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=0,z:long<-[COUNT(*)].")))
    assertResult(ir)(optimized)
  }

  test("Barbell agg with selection") {
    val ir = IR(List(Rule(
      Result(Rel("bag_1_p_a_BarbellSelAgg",Attributes(List("a")),Annotations(List("w"))),true),
        None,
        Operation("*"),
        Order(Attributes(List("p", "a"))),
        Project(Attributes(List())),
        Join(List(Rel("Edge",Attributes(List("a", "p")), Annotations(List())))),
        Aggregations(List(Aggregation("w","long",SUM(),Attributes(List("p")),"1","AGG",List()))),
        Filters(List(Selection("p",EQUALS(),"0")))),
      Rule(Result(Rel("bag_2_p_x_BarbellSelAgg",Attributes(List("x")),Annotations(List("w"))),true),
        None,
        Operation("*"),
        Order(Attributes(List("p", "x"))),
        Project(Attributes(List())),
        Join(List(Rel("Edge",Attributes(List("p", "x")),Annotations(List())))),
        Aggregations(List(Aggregation("w","long",SUM(),Attributes(List("p")),"1","AGG",List()))),
        Filters(List(Selection("p",EQUALS(),"0")))),
      Rule(Result(Rel("bag_1_x_y_z_BarbellSelAgg",Attributes(List()),Annotations(List("w"))),true),
        None,
        Operation("*"),
        Order(Attributes(List("x", "y", "z"))),
        Project(Attributes(List())),Join(List(
          Rel("Edge",Attributes(List("y", "z")),Annotations(List())),
          Rel("Edge",Attributes(List("x", "z")),Annotations(List())), Rel("Edge",Attributes(List("x", "y")),Annotations(List())),
          Rel("bag_2_p_x_BarbellSelAgg",Attributes(List("x")),Annotations(List("w"))))),
        Aggregations(List(Aggregation("w","long",SUM(),Attributes(List("x", "y", "z")),"1","AGG",List()))),
        Filters(List())),
      Rule(Result(Rel("BarbellSelAgg",Attributes(List()),Annotations(List("w"))),false),
        None,
        Operation("*"),
        Order(Attributes(List("a", "b", "c"))),Project(Attributes(List())),
        Join(List(
          Rel("Edge",Attributes(List("b", "c")),Annotations(List())),
          Rel("bag_1_x_y_z_BarbellSelAgg",Attributes(List()),Annotations(List("w"))),
          Rel("bag_1_p_a_BarbellSelAgg",Attributes(List("a")),Annotations(List("w"))),
          Rel("Edge",Attributes(List("a", "c")),Annotations(List())),
          Rel("Edge",Attributes(List("a", "b")),Annotations(List())))),
        Aggregations(List(Aggregation("w","long",SUM(),Attributes(List("a", "b", "c")),"1","AGG",List()))),
        Filters(List()))))
    val optimized = IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(DatalogParser.run("""
        BarbellSelAgg(;w) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,p),Edge(p,x),Edge(x,y),Edge(y,z),Edge(x,z),p=0,w:long<-[COUNT(*)].
    """)))
    assertResult(ir)(optimized)
  }

  test("Pagerank, test that usedScalar field is filled out properly") {
    val ir = IR(List(Rule(Result(
      Rel("N",Attributes(List()),Annotations(List("w"))),false),
      None,
      Operation("*"),
      Order(Attributes(List("x", "y"))),
      Project(Attributes(List("y"))),
      Join(List(Rel("Edge",Attributes(List("x", "y")),Annotations(List())))),
      Aggregations(List(Aggregation("w","long",SUM(),Attributes(List("x")),"1","AGG",List()))),Filters(List())),
    Rule(Result(
      Rel("PageRank_basecase",Attributes(List("x")),Annotations(List("y"))),true),
      None,
      Operation("*"),
      Order(Attributes(List("x", "z"))),
      Project(Attributes(List())),
      Join(List(Rel("Edge",Attributes(List("x", "z")), Annotations(List())))),
      Aggregations(List(Aggregation("y","float",CONST(),Attributes(List("z")),"(1.0/N)","",List(Rel("N",Attributes(List()),Annotations(List("w"))))))),Filters(List())),
    Rule(Result(
      Rel("PageRank",Attributes(List("x")),Annotations(List("y"))),false),
      Some(Recursion(ITERATIONS(),EQUALS(),"5")),
      Operation("*"),
      Order(Attributes(List("x", "z"))),
      Project(Attributes(List())),
      Join(List(
        Rel("PageRank_basecase",Attributes(List("z")),Annotations(List())),
        Rel("Edge",Attributes(List("x", "z")),Annotations(List())),
        Rel("InvDegree",Attributes(List("z")),Annotations(List())))),
      Aggregations(List(Aggregation("y","float",SUM(),Attributes(List("z")),"1.0","0.15+0.85*AGG",List()))),
      Filters(List()))))
    val pgrank = """
      N(;w) :- Edge(x,y),w:long<-[SUM(x;1)].
      PageRank(x;y) :- Edge(x,z),y:float<-[(1.0/N)].
      PageRank(x;y)*[i=5]:-Edge(x,z),PageRank(z),InvDegree(z),y:float <- [0.15+0.85*SUM(z;1.0)]."""
    val optimized = IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(DatalogParser.run(pgrank)))
    assertResult(ir)(optimized)
  }

  test("sssp") {
    val ir = IR(List(
      Rule(Result(
        Rel("SSSP_basecase",Attributes(List("x")),Annotations(List("y"))),true),None,Operation("*"),Order(Attributes(List("w", "x"))),
        Project(Attributes(List())),
        Join(List(Rel("Edge",Attributes(List("w", "x")),Annotations(List())))),
        Aggregations(List(Aggregation("y","long",CONST(),Attributes(List("w")),"1","",List()))),
        Filters(List(Selection("w",EQUALS(),"0")))),
      Rule(Result(Rel("SSSP",Attributes(List("x")),Annotations(List("y"))),false),
        Some(Recursion(EPSILON(),EQUALS(),"0")),Operation("*"),Order(Attributes(List("x", "w"))),
        Project(Attributes(List())),
        Join(List(Rel("SSSP_basecase",Attributes(List("w")),Annotations(List())), Rel("Edge",Attributes(List("w", "x")),Annotations(List())))),
        Aggregations(List(Aggregation("y","long",MIN(),Attributes(List("w")),"1","1+AGG",List()))),Filters(List()))))
    val sssp =
      """SSSP(x;y) :- Edge(w,x),w=0,y:long <- [1].
         SSSP(x;y)*[c=0] :- Edge(w,x),SSSP(w),y:long <- [1+MIN(w;1)]."""
    val optimized = IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(DatalogParser.run(sssp)))
    assertResult(ir)(optimized)
  }

  test("lubm1, 2 join w/ 2 selects") {
    val ir = IR(List(
      Rule(Result(Rel("lubm1",Attributes(List("a")),Annotations(List())),false),
        None,
        Operation("*"),
        Order(Attributes(List("b", "c", "a"))),Project(Attributes(List("b", "c"))),
        Join(List(
          Rel("takesCourse",Attributes(List("a", "b")),Annotations(List())),
          Rel("rdftype",Attributes(List("a", "c")),Annotations(List())))),
        Aggregations(List()),
        Filters(List(Selection("b",EQUALS(),"'http://www.Department0.University0.edu/GraduateCourse0'"),
          Selection("c",EQUALS(),"'http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent'")
        )))))
    val lubm1 = """
        lubm1(a) :- takesCourse(a,b),rdftype(a,c),b='http://www.Department0.University0.edu/GraduateCourse0',c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent'."""
    val optimized = IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(DatalogParser.run(lubm1)))
    assertResult(ir)(optimized)
  }

  test("lumb2, triangle with a selected rel at each corner") {
    val ir = IR(List(
      Rule(Result(Rel("bag_1_y_b_lubm2",Attributes(List("b")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("y", "b"))),
        Project(Attributes(List("y"))),
        Join(List(Rel("rdftype",Attributes(List("b", "y")), Annotations(List())))),Aggregations(List()),
        Filters(List(Selection("y",EQUALS(),"'http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department'")))),
      Rule(Result(Rel("bag_1_z_c_lubm2",Attributes(List("c")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("z", "c"))),
        Project(Attributes(List("z"))),Join(List(Rel("rdftype",Attributes(List("c", "z")),Annotations(List())))),Aggregations(List()),
        Filters(List(Selection("z",EQUALS(),"'http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University'")))),
      Rule(Result(Rel("bag_1_x_a_lubm2",Attributes(List("a")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("x", "a"))),
        Project(Attributes(List("x"))),Join(List(Rel("rdftype",Attributes(List("a", "x")),Annotations(List())))),Aggregations(List()),
        Filters(List(Selection("x",EQUALS(),"'http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent'")))),
      Rule(Result(Rel("lubm2",Attributes(List("a", "b", "c")),Annotations(List())),false),
        None,
        Operation("*"),
        Order(Attributes(List("a", "b", "c"))),Project(Attributes(List())),
        Join(List(
          Rel("memberOf",Attributes(List("a", "b")),Annotations(List())),
          Rel("bag_1_z_c_lubm2",Attributes(List("c")),Annotations(List())),
          Rel("undegraduateDegreeFrom",Attributes(List("a", "c")),Annotations(List())),
          Rel("subOrganizationOf",Attributes(List("b", "c")),Annotations(List())),
          Rel("bag_1_x_a_lubm2",Attributes(List("a")),Annotations(List())),
          Rel("bag_1_y_b_lubm2",Attributes(List("b")), Annotations(List())))),
        Aggregations(List()),
        Filters(List()))))
    val lubm2 = """ |lubm2(a,b,c) :- memberOf(a,b),
        |subOrganizationOf(b,c),
        |undegraduateDegreeFrom(a,c),
        |rdftype(a,x),
        |x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent',
        |rdftype(c,z),
        |z='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University',
        |rdftype(b,y),
        |y='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department'.""".stripMargin.replaceAll("\n", "")
    val optimized = IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(DatalogParser.run(lubm2)))
    assertResult(ir)(optimized)
  }

  test("lubm4, check that we don't actually instruct you to compute duplicated/pushed-down selected rels twice") {
    val ir = IR(List(
      Rule(
        Result(Rel("bag_2_f_a_lubm4",Attributes(List("a")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("f", "a"))),
        Project(Attributes(List("f"))),
        Join(List(Rel("rdftype", Attributes(List("a", "f")),Annotations(List())))),
        Aggregations(List()),
        Filters(List(Selection("f",EQUALS(),"'http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor'")))),
      Rule(Result(Rel("bag_2_e_a_lubm4",Attributes(List("a")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("e", "a"))),
        Project(Attributes(List("e"))),
        Join(List(
          Rel("worksFor",Attributes(List("a", "e")),Annotations(List())))),
        Aggregations(List()),Filters(List(Selection("e",EQUALS(),"'http://www.Department0.University0.edu'")))),
      Rule(Result(Rel("bag_1_a_b_lubm4",Attributes(List("a", "b")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("a", "b"))),
        Project(Attributes(List())),
        Join(List(
          Rel("name",Attributes(List("a", "b")),Annotations(List())),
          Rel("bag_2_e_a_lubm4",Attributes(List("a")),Annotations(List())),
          Rel("bag_2_f_a_lubm4",Attributes(List("a")),Annotations(List())))),
        Aggregations(List()),
        Filters(List())),
      Rule(Result(Rel("bag_1_a_c_lubm4",Attributes(List("a", "c")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("a", "c"))),
        Project(Attributes(List())),
        Join(List(
          Rel("telephone",Attributes(List("a", "c")),Annotations(List())),
          Rel("bag_2_e_a_lubm4",Attributes(List("a")),Annotations(List())),
          Rel("bag_2_f_a_lubm4",Attributes(List("a")),Annotations(List())))),Aggregations(List()),Filters(List())),
      Rule(Result(Rel("lubm4_root",Attributes(List("a", "d")),Annotations(List())),true),
        None,
        Operation("*"),Order(Attributes(List("a", "d", "b", "c"))),
        Project(Attributes(List("b", "c"))),
        Join(List(
          Rel("emailAddress",Attributes(List("a", "d")),Annotations(List())),
          Rel("bag_1_a_b_lubm4",Attributes(List("a", "b")),Annotations(List())),
          Rel("bag_1_a_c_lubm4",Attributes(List("a", "c")),Annotations(List())))),Aggregations(List()),Filters(List())),
      Rule(Result(Rel("lubm4",Attributes(List("a", "b", "c", "d")),Annotations(List())),false),
        None,
        Operation("*"),
        Order(Attributes(List("a", "d", "b", "c"))),
        Project(Attributes(List())),
        Join(List(
          Rel("lubm4_root",Attributes(List("a", "d")),Annotations(List())),
          Rel("bag_1_a_b_lubm4",Attributes(List("a", "b")),Annotations(List())),
          Rel("bag_1_a_c_lubm4",Attributes(List("a", "c")),Annotations(List())))),
        Aggregations(List()),
        Filters(List()))))
    val lubm4 =
      """
        |lubm4(a,b,c,d) :- worksFor(a,e),
        |e='http://www.Department0.University0.edu',
        |name(a,b),
        |emailAddress(a,d),
        |telephone(a,c),
        |rdftype(a,f),
        |f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor'.""".stripMargin
    assertResult(ir)(IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(DatalogParser.run(lubm4))))
  }

  test("lubm7") {
    val ir = IR(List(
      Rule(Result(Rel("bag_1_e_a_lubm7",Attributes(List("a")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("e", "a"))),
        Project(Attributes(List("e"))),
        Join(List(
          Rel("rdftype",Attributes(List("a", "e")),Annotations(List())))),Aggregations(List()),
        Filters(List(Selection("e",EQUALS(),"'http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent'")))),
      Rule(Result(Rel("bag_1_d_b_lubm7",Attributes(List("b")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("d", "b"))),
        Project(Attributes(List("d"))),
        Join(List(
          Rel("rdftype",Attributes(List("b", "d")),Annotations(List())))),
        Aggregations(List()),
        Filters(List(Selection("d",EQUALS(),"'http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course'")))),
      Rule(Result(Rel("bag_1_c_b_lubm7",Attributes(List("b")),Annotations(List())),true),
        None,
        Operation("*"),Order(Attributes(List("c", "b"))),Project(Attributes(List("c"))),
        Join(List(Rel("teacherOf",Attributes(List("c", "b")),Annotations(List())))),
        Aggregations(List()),
        Filters(List(Selection("c",EQUALS(),"'http://www.Department0.University0.edu/AssociateProfessor0'")))),
      Rule(Result(Rel("lubm7",Attributes(List("a", "b")),Annotations(List())),false),
        None,
        Operation("*"),
        Order(Attributes(List("b", "a"))),
        Project(Attributes(List())),
        Join(List(
          Rel("takesCourse",Attributes(List("a", "b")),Annotations(List())),
          Rel("bag_1_c_b_lubm7",Attributes(List("b")),Annotations(List())),
          Rel("bag_1_d_b_lubm7",Attributes(List("b")),Annotations(List())),
          Rel("bag_1_e_a_lubm7",Attributes(List("a")),Annotations(List())))),
        Aggregations(List()),
        Filters(List()))))
    val lubm7 =
      """
        |lubm7(a,b) :- teacherOf(c,b),
        |c='http://www.Department0.University0.edu/AssociateProfessor0',
        |takesCourse(a,b),
        |rdftype(b,d),
        |d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course',
        |rdftype(a,e),
        |e='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent'.
      """.stripMargin
    val optimized = IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(DatalogParser.run(lubm7)))
    assertResult(ir)(optimized)
  }

  test("lubm8") {
    val ir = IR(List(
      Rule(Result(Rel("bag_2_d_a_lubm8",Attributes(List("a")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("d", "a"))),
        Project(Attributes(List("d"))),
        Join(List(Rel("rdftype",Attributes(List("a", "d")),Annotations(List())))),
        Aggregations(List()),
        Filters(List(Selection("d",EQUALS(),"'http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent'")))),
      Rule(Result(Rel("bag_2_f_b_lubm8",Attributes(List("b")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("f", "b"))),
        Project(Attributes(List("f"))),
        Join(List(Rel("rdftype",Attributes(List("b", "f")),Annotations(List())))),
        Aggregations(List()),
        Filters(List(Selection("f",EQUALS(),"'http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department'")))),
      Rule(Result(Rel("bag_2_e_b_lubm8",Attributes(List("b")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("e", "b"))),
        Project(Attributes(List("e"))),
        Join(List(Rel("subOrganizationOf",Attributes(List("b", "e")),Annotations(List())))),
        Aggregations(List()),
        Filters(List(Selection("e",EQUALS(),"'http://www.University0.edu'")))),
      Rule(Result(Rel("bag_1_a_b_lubm8",Attributes(List("a", "b")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("a", "b"))),
        Project(Attributes(List())),
        Join(List(
          Rel("memberOf",Attributes(List("a", "b")),Annotations(List())),
          Rel("bag_2_e_b_lubm8",Attributes(List("b")),Annotations(List())),
          Rel("bag_2_f_b_lubm8",Attributes(List("b")),Annotations(List())),
          Rel("bag_2_d_a_lubm8",Attributes(List("a")),Annotations(List())))),
        Aggregations(List()),Filters(List())),
      Rule(Result(Rel("lubm8_root",Attributes(List("a", "c")),Annotations(List())),true),
        None,
        Operation("*"),
        Order(Attributes(List("a", "c", "b"))),
        Project(Attributes(List("b"))),
        Join(List(
          Rel("emailAddress",Attributes(List("a", "c")),Annotations(List())),
          Rel("bag_1_a_b_lubm8",Attributes(List("a", "b")),Annotations(List())))),
        Aggregations(List()),Filters(List())),
      Rule(Result(Rel("lubm8",Attributes(List("a", "b", "c")),Annotations(List())),false),
        None,
        Operation("*"),
        Order(Attributes(List("a", "c", "b"))),
        Project(Attributes(List())),
        Join(List(
          Rel("lubm8_root",Attributes(List("a", "c")),Annotations(List())),
          Rel("bag_1_a_b_lubm8",Attributes(List("a", "b")),Annotations(List())))),
        Aggregations(List()),
        Filters(List()))))
    val lubm8 =
      """
        |lubm8(a,b,c) :- memberOf(a,b),
        |emailAddress(a,c),
        |rdftype(a,d),
        |d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent',
        |subOrganizationOf(b,e),
        |e='http://www.University0.edu',
        |rdftype(b,f),
        |f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department'.
        |""".stripMargin
    assertResult(ir)(IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(DatalogParser.run(lubm8))))
  }
}
