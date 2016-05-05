package duncecap

import org.scalatest.FunSuite

class SQLPlannerTest extends FunSuite {

  val relationsMap = scala.collection.mutable.Map(
    "lineitem" -> Relation("lineitem", Schema(
      //      externalAttributeTypes = List("uint32", "uint32", "uint32", "int32",
      //        "string", "string", "date", "date", "date",
      //        "string", "string", "string"),
      externalAttributeTypes = List(),
      externalAnnotationTypes = List("float32", "float32", "float32", "float32"),
      attributeNames = List("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber",
        "l_returnflag", "l_linestatus", "l_shipdate", "l_commitdate", "l_receiptdate",
        "l_shipinstruct", "l_shipmode", "l_comment"),
      annotationNames = List("l_quantity", "l_extendedprice", "l_discount", "l_tax")
    ), "", false),
    "supplier" -> Relation("supplier", Schema(
      //      externalAttributeTypes = List("uint32", "string", "string", "uint32", "string", "string"),
      externalAttributeTypes = List(),
      externalAnnotationTypes = List("float"),
      attributeNames = List("s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_comment", "s_acctbal"),
      annotationNames = List()
    ), "", false),
    "partsupp" -> Relation("partsupp", Schema(
      //      externalAttributeTypes = List("uint32", "uint32", "string"),
      externalAttributeTypes = List(),
      //      externalAnnotationTypes = List("int32", "float"),
      externalAnnotationTypes = List(),
      attributeNames = List("ps_partkey", "ps_suppkey", "ps_comment"),
      annotationNames = List("ps_availqty", "ps_supplycost")
    ), "", false),
    "nation" -> Relation("nation", Schema(
      //      externalAttributeTypes = List("uint32", "string", "uint32", "string"),
      externalAttributeTypes = List(),
      externalAnnotationTypes = List(),
      attributeNames = List("n_nationkey", "n_name", "n_regionkey", "n_comment"),
      annotationNames = List()
    ), "", false),
    "region" -> Relation("region", Schema(
      //      externalAttributeTypes = List("uint32", "string", "string"),
      externalAttributeTypes = List(),
      externalAnnotationTypes = List(),
      attributeNames = List("r_regionkey", "r_name", "r_comment"),
      annotationNames = List()
    ), "", false),
    "part" -> Relation("part", Schema(
      //      externalAttributeTypes = List("uint32", "string", "string",
      //      "string", "string", "int32", "string", "string"),
      externalAttributeTypes = List(),
      externalAnnotationTypes = List("float"),
      attributeNames = List("p_partkey", "p_name", "p_mfgr",
        "p_brand", "p_type", "p_size", "p_container", "p_comment"),
      annotationNames = List("p_retailprice")
    ), "", false),
    "customer" -> Relation("customer", Schema(
      externalAttributeTypes = List(),
      externalAnnotationTypes = List("float"),
      attributeNames = List("c_custkey", "c_name", "c_address", "c_nationkey",
        "c_phone", "c_mktsegment", "c_comment"),
      annotationNames = List("c_acctbal")
    ), "", false),
    "orders" -> Relation("orders", Schema(
      externalAttributeTypes = List(),
      externalAnnotationTypes = List(),
      attributeNames = List("o_orderkey", "o_custkey", "o_orderstatus",
        "o_totalprice", "o_orderdate", "o_orderpriority", "o_clerk", "o_shippriority",
        "o_comment", "o_year"),
      annotationNames = List()
    ), "", false)
  )

  def checkJoins(rules: List[Rule]) = {
    rules.foreach(rule => {
      if (rule.join.rels.length > 1) {
        rule.join.rels.foreach(rel => {
          val valid = rule.join.rels.exists(otherRel =>
            rel != otherRel && rel.attrs.values.intersect(otherRel.attrs.values).nonEmpty
          )
          assert(valid, s"Joins broken in bag ${rule.result.rel.name} on relation ${rel.name}.")
        })
      }
    })
  }

  test("q1") {
    // This query is the same as in the TPC docs.
    val query =
      """
        |CREATE TABLE q1 AS (
        |  SELECT
        |    l_returnflag,
        |    l_linestatus,
        |    sum(l_quantity)                                       AS sum_qty,
        |    sum(l_extendedprice)                                  AS sum_base_price,
        |    sum(l_extendedprice * (1 - l_discount))               AS sum_disc_price,
        |    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
        |    avg(l_quantity)                                       AS avg_qty,
        |    avg(l_extendedprice)                                  AS avg_price,
        |    avg(l_discount)                                       AS avg_disc,
        |    count(*)                                              AS count_order
        |  FROM
        |    lineitem
        |  WHERE
        |    l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
        |GROUP BY l_returnflag,
        |  l_linestatus ORDER BY
        |  l_returnflag, l_linestatus);
      """.stripMargin
    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // Return a one bag GHD with lineitem in it. Check that the annotations appear on the lineitem rel.
    assertResult(1)(optim.getNumRules())
    assertResult(List("lineitem"))(optim.rules.head.join.rels.map(_.name))
    assertResult(Set("l_quantity", "l_extendedprice", "l_discount", "l_tax"))(
      optim.rules.head.join.rels.head.anno.values.toSet
    )

    val rule = optim.rules.head
    // Check output attributes and annotations.
    // l_returnflag and l_linestatus should be the only attributes in the result.
    assertResult(Set("l_returnflag", "l_linestatus"))(rule.result.rel.attrs.values.toSet)
    val annotation_names = Set("sum_qty", "sum_base_price", "sum_disc_price", "sum_charge", "avg_qty", "avg_price",
      "avg_disc", "count_order")
    // Everything else should be an annotation.
    assertResult(annotation_names)(rule.result.rel.anno.values.toSet)
    // Check aggregations. The expression inside sums is composed of annotations on the lineitem table. The attrs. on
    // each aggregation should be every attr. on the lineitem table except l_returnflag and l_linestatus, which are
    // output.
    val innerExpressions = Set("l_quantity", "l_extendedprice", "l_extendedprice*(1-l_discount)",
      "l_extendedprice*(1-l_discount)*(1+l_tax)", "l_quantity", "l_extendedprice", "l_discount", "")
    val summedOut = Set("l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_commitdate",
      "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment")
    assertResult(annotation_names)(rule.aggregations.values.map(_.annotation).toSet)
    assertResult(innerExpressions)(rule.aggregations.values.map(_.innerExpression match {
      case LiteralExpr(v, _, _) => v
    }).toSet)
    rule.aggregations.values.foreach(agg => assertResult(summedOut)(agg.attrs.values.toSet))
    // Check filters. Date arithmetic is done by the parser. Selections are more complicated than in datalog, so the
    // rhs is a wrapped in a SelectionValue class so we can have dates, lists, OR, etc.
    assertResult(List(Selection(
      "l_shipdate", LTE(), SelectionDate(SQLParser.dateFormat.parse("1998-09-02"))
    )))(rule.filters.values)
    // Check that order by gets passed through
    assertResult(Set(OrderingTerm("l_returnflag", false), OrderingTerm("l_linestatus", false)))(rule.orderBy.get.terms.toSet)
  }

  test("q2") {
    // This was originally a correlated subquery.
    val query =
      """
        |CREATE TABLE sq AS (
        |  SELECT
        |    ps_partkey         AS sq_ps_partkey,
        |    min(ps_supplycost) AS sq_min
        |  FROM
        |    partsupp, supplier,
        |    nation, region
        |  WHERE
        |    s_suppkey = ps_suppkey AND s_nationkey = n_nationkey AND
        |    n_regionkey = r_regionkey AND r_name = 'EUROPE'
        |  GROUP BY ps_partkey);
        |
        |CREATE TABLE q2 AS (
        |  SELECT
        |    s_acctbal,
        |    s_name,
        |    n_name,
        |    p_partkey,
        |    p_mfgr,
        |    s_address,
        |    s_phone,
        |    s_comment
        |  FROM
        |    part,
        |    supplier, partsupp, nation, region, sq
        |  WHERE
        |    p_partkey = ps_partkey
        |    AND s_suppkey = ps_suppkey AND p_size = 15
        |    AND p_type LIKE '%BRASS'
        |    AND s_nationkey = n_nationkey AND
        |    n_regionkey = r_regionkey AND r_name = 'EUROPE'
        |    AND sq_ps_partkey = p_partkey
        |    AND ps_supplycost = sq_min
        |  ORDER BY
        |    s_acctbal DESC, n_name, s_name, p_partkey);
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    IROptimizer.fixTopDownPass(optim)
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // The first subquery should not require a top-down pass, just a join tree with partsupp at the top.
    assertResult(Rel("sq", Attributes(List("ps_partkey")), Annotations(List("sq_min"))))(optim.rules(3).result.rel)

    // Check that the selection on the annotation goes through the optimizer
    assert(optim.rules.flatMap(_.filters.values).contains(Selection("ps_supplycost", EQUALS(), SelectionLiteral("sq_min"))))
  }

  test("q3") {
    // This query is the same as the TPC docs.
    val query =
      """
        |CREATE TABLE q3 AS (
        |  SELECT
        |    l_orderkey,
        |    sum(l_extendedprice * (1 - l_discount)) AS revenue,
        |    o_orderdate,
        |    o_shippriority
        |  FROM
        |    customer,
        |    orders,
        |    lineitem
        |  WHERE
        |    c_mktsegment = 'BUILDING' AND
        |    c_custkey = o_custkey AND
        |    l_orderkey = o_orderkey AND
        |    o_orderdate < DATE '1995-03-15' AND
        |    l_shipdate > DATE '1995-03-15'
        |  GROUP BY l_orderkey,
        |    o_orderdate,
        |    o_shippriority
        |  ORDER BY
        |    revenue DESC, o_orderdate);
        |    """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // The GHD should have one bag for each relation and then a top-down pass.
    assertResult(4)(optim.rules.length)

    // The GHD should have l_extendedprice and l_discount on lineitem when it is joined in, but the bag where it is
    // joined in should not have these as annotations, because the aggregation is done on this bag.
    val lineitemBag = optim.rules.find(_.join.rels.exists(rel => rel.name == "lineitem")).get
    assertResult(Set("l_extendedprice", "l_discount"))(
      lineitemBag.join.rels.find(_.name == "lineitem").get.anno.values.toSet
    )
    assert(Set("l_extendedprice", "l_discount").intersect(lineitemBag.result.rel.anno.values.toSet).isEmpty)

    // Check the result has the expected attributes and annotations.
    val result = optim.rules.last
    assertResult(Set("l_orderkey_o_orderkey", "o_orderdate", "o_shippriority"))(result.result.rel.attrs.values.toSet)
    assertResult(List("revenue"))(result.result.rel.anno.values)

    // lineitem is joined in on the last bag, and we sum over every attribute except l_orderkey, which is output, and
    // l_shipdate, which is selected
    val agg = optim.rules(2).aggregations.values.head
    assertResult(
      relationsMap("lineitem").schema.attributeNames.toSet -- Set("l_orderkey", "l_shipdate")
    )(agg.attrs.values.toSet)
    // The only aggregation should be on the lineitem bag
    optim.rules.foreach(rule => {
      if (!rule.join.rels.map(_.name).contains("lineitem")) assert(rule.aggregations.values.isEmpty)
    })
  }

  test("q4") {
    // This was originally a correlated subquery, it will work because EH currently does not allow duplicates in
    // projections.
    val query =
      """
        |CREATE TABLE sq AS (
        |  SELECT DISTINCT l_orderkey
        |  FROM
        |    lineitem
        |  WHERE
        |    l_commitdate < l_receiptdate
        |);
        |
        |CREATE TABLE q4 AS (
        |  SELECT
        |    o_orderpriority,
        |    count(*) AS order_count
        |  FROM
        |    orders, sq
        |  WHERE
        |    o_orderdate >= DATE '1993-07-01'
        |    AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH AND
        |                                                             o_orderkey = l_orderkey GROUP BY
        |                                                             o_orderpriority ORDER BY
        |                                                             o_orderpriority
        |);
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // Each query should just be a single bag.
    assertResult(2)(optim.rules.length)
    // The second query should be a bag with both order and sq in it.
    assertResult(Set("orders", "sq"))(optim.rules.last.join.rels.map(_.name).toSet)
    // Check the count aggregation.
    val agg = optim.rules.last.aggregations.values.head
    assertResult("order_count")(agg.annotation)
    assertResult(SUM())(agg.operation)
    assertResult(Set(
      "o_orderkey_l_orderkey", "o_shippriority", "o_custkey", "o_year", "o_clerk", "o_orderstatus",
      "o_totalprice", "o_comment"
    ))(agg.attrs.values.toSet)
    assertResult("")(agg.innerExpression.asInstanceOf[LiteralExpr].value)
    // Check the joins actually line up.
    val sqJoinRel = optim.rules.last.join.rels.find(_.name == "sq").get
    val orderJoinRel = optim.rules.last.join.rels.find(_.name == "orders").get
    assert(orderJoinRel.attrs.values.contains(sqJoinRel.attrs.values.head))
  }

  test("q5") {
    val query =
      """
        |CREATE TABLE q5 AS (
        |  SELECT
        |    n_name,
        |    sum(l_extendedprice * (1 - l_discount)) AS revenue
        |  FROM
        |    customer, orders, lineitem, supplier, nation, region
        |  WHERE
        |    c_custkey = o_custkey
        |    AND l_orderkey = o_orderkey
        |    AND l_suppkey = s_suppkey
        |    AND c_nationkey = s_nationkey
        |    AND s_nationkey = n_nationkey
        |    AND n_regionkey = r_regionkey
        |    AND r_name = '[REGION]'
        |    AND o_orderdate >= DATE '1998-12-01'
        |    AND o_orderdate < DATE '1998-12-01' + INTERVAL '1' YEAR GROUP BY
        |  n_name ORDER BY
        |  revenue DESC);
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    // This query is a square, the edge cover should be 2.
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(2.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // Check three filters go through
    assertResult(3)(optim.rules.flatMap(_.filters.values).length)
    // Should produce a GHD with 5 bags
    assertResult(4)(optim.rules.length)
    // Check the aggregation only shows up on the bag with lineitem and supplier in it.
    optim.rules.foreach(rule => {
      if (rule.join.rels.map(_.name).contains("lineitem")) {
        val agg = rule.aggregations.values.head
        assertResult("revenue")(agg.annotation)
        assertResult("long")(agg.datatype)
        assertResult(SUM())(agg.operation)
        assertResult(List("s_phone", "l_returnflag", "l_comment", "l_linestatus", "s_address", "l_shipmode",
          "l_shipinstruct", "l_receiptdate", "s_name", "l_linenumber", "s_acctbal", "l_shipdate",
          "l_orderkey_o_orderkey", "l_partkey", "s_comment", "l_commitdate", "l_suppkey_s_suppkey"))(agg.attrs.values)
        assertResult("1")(agg.init)
        assertResult("AGG")(agg.expression)
        assertResult(List())(agg.usedScalars)
        assertResult("l_extendedprice*(1-l_discount)")(agg.innerExpression.asInstanceOf[LiteralExpr].value)
      }
    })
  }

  test("q6") {
    val query =
      """
        |CREATE TABLE q6 AS (
        |  SELECT sum(l_extendedprice * l_discount) AS revenue
        |  FROM
        |    lineitem
        |  WHERE
        |    l_shipdate >= DATE '1998-12-01'
        |    AND l_shipdate < DATE '1998-12-01' + INTERVAL '1' YEAR
        |  AND l_discount BETWEEN 1 - 0.01 AND 2 + 0.01 AND l_quantity < 20);
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // This should just be a GHD with one bag.
    assertResult(1)(optim.rules.length)
    // Check the result and the aggregation.
    val result = Result(Rel("q6", Attributes(List()), Annotations(List("revenue"))), isIntermediate = false)
    assertResult(result)(optim.rules.head.result)

    val agg = optim.rules.head.aggregations.values.head
    assertResult("revenue")(agg.annotation)
    assertResult("long")(agg.datatype)
    assertResult(SUM())(agg.operation)
    assertResult(List("l_returnflag", "l_comment", "l_linestatus", "l_shipmode", "l_shipinstruct", "l_receiptdate",
      "l_linenumber", "l_partkey", "l_commitdate", "l_suppkey", "l_orderkey"))(agg.attrs.values)
    assertResult("1")(agg.init)
    assertResult("AGG")(agg.expression)
    assertResult(List())(agg.usedScalars)
    assertResult("l_extendedprice*l_discount")(agg.innerExpression.asInstanceOf[LiteralExpr].value)

    // Check all the filters went through (between gets split into two filters).
    assertResult(5)(optim.rules.head.filters.values.length)
  }

  test("q7") {
    // Rewrote without subquery.
    val query =
      """
        |CREATE TABLE q7 AS (
        |  SELECT
        |    n1.n_name                               AS supp_nation,
        |    n2.n_name                               AS cust_nation,
        |    l_year,
        |    sum(l_extendedprice * (1 - l_discount)) AS volume
        |  FROM
        |    supplier,
        |    lineitem, orders, customer, nation n1, nation n2
        |  WHERE
        |    s_suppkey = l_suppkey
        |    AND o_orderkey = l_orderkey
        |    AND c_custkey = o_custkey
        |    AND s_nationkey = n1.n_nationkey AND c_nationkey = n2.n_nationkey AND (
        |      (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
        |    )
        |    AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        |  GROUP BY supp_nation,
        |    cust_nation,
        |    l_year
        |  ORDER BY
        |    supp_nation, cust_nation, l_year);
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    // Because there is a selection that touches both nation relations, they must be in the same bag. This is why the
    // FHW is 2.0.
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // Check that the two nation rels are in one bag and the OR filter is there.
    val nationBag = optim.rules.find(rule => {
      val bagRels = rule.join.rels.map(_.name)
      bagRels.count(_ == "nation") == 2
    })
    assert(nationBag.isDefined)
    val lhsSelections = Filters(List(
      Selection("n1_n_name", EQUALS(), SelectionLiteral("\"FRANCE\"")),
      Selection("n2_n_name", EQUALS(), SelectionLiteral("\"GERMANY\""))
    ))
    val rhsSelections = Filters(List(
    Selection("n1_n_name", EQUALS(), SelectionLiteral("\"GERMANY\"")),
    Selection("n2_n_name", EQUALS(), SelectionLiteral("\"FRANCE\""))
    ))
    val orFilter = nationBag.get.filters.values.find(filter => {
      filter.operation == OR()
    })
    assert(orFilter.isDefined)
    assertResult(SelectionOrList(List(lhsSelections, rhsSelections)))(orFilter.get.value)
  }

  test("q8") {
    // Rewrote to not divide the annotations (i.e. have the nation volume and total, not the percentage).
    // Also, removed EXTRACT.

    val query =
      """
        |CREATE TABLE all_nations AS (
        |  SELECT
        |    o_year,
        |    l_extendedprice * (1 - l_discount) AS volume,
        |    n2.n_name                          AS n2_n_name
        |  FROM
        |    part,
        |    supplier, lineitem, orders, customer, nation n1, nation n2, region
        |  WHERE
        |    p_partkey = l_partkey
        |    AND s_suppkey = l_suppkey
        |    AND l_orderkey = o_orderkey
        |    AND o_custkey = c_custkey
        |    AND c_nationkey = n1.n_nationkey AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA'
        |    AND s_nationkey = n2.n_nationkey
        |    AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31' AND p_type = 'ECONOMY ANODIZED STEEL'
        |);
        |
        |CREATE TABLE q8 AS (
        |  SELECT
        |    o_year,
        |    sum(CASE
        |        WHEN n2_n_name = 'BRAZIL'
        |          THEN volume
        |        ELSE 0
        |        END)    AS nation_volume,
        |    sum(volume) AS total_volume
        |  FROM all_nations
        |  GROUP BY
        |    o_year
        |  ORDER BY
        |    o_year
        |);
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    // AJAR creates an "imaginary" edge over o_year and n2.n_name, so the FHW is 2.0
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(2.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // Check that the aggregation happens in a bag with lineitem.
    optim.rules.foreach(rule => {
      if (rule.result.rel.name == "lineitem") {
        val aggregation = Aggregation(
          "nation_volume",
          "long",
          CONST(),
          Attributes(List("n2_n_name")),
          "1",
          "l_extendedprice*(1-l_discount)",
          List(),
          LiteralExpr("", List())
        )
        assertResult(Aggregations(List(aggregation)))(rule.aggregations)
      }
    })

    // The second query should just be a single bag with two aggregations.
    val result = Result(
      Rel("q8", Attributes(List("o_year")), Annotations(List("nation_volume", "total_volume"))),
      isIntermediate = false
    )
    assertResult(result)(optim.rules.last.result)

    assert(
      optim.rules.last.aggregations.values.count(agg => {
        agg.innerExpression match {
          case LiteralExpr(value, _, _) => value == "volume"
          case CaseExpr(condition, ifCase, elseCase, _, _) => {
            condition == List(Selection("n2_n_name", EQUALS(), SelectionLiteral("\"BRAZIL\""), negated = false)) &&
              ifCase == "volume" &&
              elseCase == "0"
          }
        }
      }) == 2
    )
  }

  test("q9") {
    // Rewrote as one query and removed EXTRACT.
    val query =       """
                        |SELECT
                        |  n_name                                                               AS nation,
                        |  o_year,
                        |  sum(l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity) AS sum_profit
                        |FROM
                        |  part, supplier, lineitem, partsupp, orders, nation
                        |WHERE
                        |  s_suppkey = l_suppkey
                        |  AND ps_suppkey = l_suppkey AND ps_partkey = l_partkey
                        |  AND p_partkey = l_partkey
                        |  AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey AND p_name LIKE '%green%'
                        |GROUP BY
                        |  nation,
                        |  o_year
                        |ORDER BY
                        |  nation, o_year DESC
                      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(2.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // Because lineitem and partsupp have annotations in the aggregation, the aggregation must occur at the lowest bag
    // where lineitem and partsupp have been joined in.
    val aggBag = optim.rules.find(rule => rule.aggregations.values.map(_.annotation).contains("sum_profit")).get
    // Check lineitem or partsupp is in the bag where the aggregation starts.
    assert(aggBag.join.rels.map(_.name).toSet.intersect(Set("lineitem", "partsupp")).nonEmpty)
    // Check that lineitem and partsupp are both below the bag.
    val rulesBefore = optim.rules.takeWhile(_ != aggBag) :+ aggBag
    assert(Set("lineitem", "partsupp").subsetOf(rulesBefore.flatMap(rule => rule.join.rels.map(_.name)).toSet))

    // The aggregation on the first bag should have the full inner expression.
    val firstAgg = aggBag.aggregations.values.head
    assertResult(firstAgg.annotation)("sum_profit")
    assertResult(firstAgg.innerExpression.asInstanceOf[LiteralExpr].value)("l_extendedprice*(1-l_discount)-ps_supplycost*l_quantity")

    // The later aggregations should just use the annotation.
    optim.rules.foreach(rule =>
      if (rule.join.rels.map(_.name).contains(aggBag.result.rel.name)) {
        val laterAgg = rule.aggregations.values.head
        assertResult(laterAgg.annotation)("sum_profit")
        assertResult(laterAgg.innerExpression.asInstanceOf[LiteralExpr].value)("sum_profit")
        assert(rule.join.rels.flatMap(_.anno.values).contains("sum_profit"))
      }
    )

    // partsupp occurs lower down, so we need ps_supplycost to show up on the partsupp bag, and then get passed up
    // until it is aggregated out.
    val partsuppBag = optim.rules.find(_.join.rels.exists(rel => rel.name == "partsupp")).get
    assertResult("ps_supplycost")(partsuppBag.join.rels.find(_.name == "partsupp").get.anno.values.head)
    assert(partsuppBag.result.rel.anno.values.contains("ps_supplycost"))

    val partsuppBagName = partsuppBag.result.rel.name
    val aggregatedBag = optim.rules.find(_.join.rels.exists(rel => rel.name == partsuppBagName)).get
    assertResult("ps_supplycost")(aggregatedBag.join.rels.find(_.name == partsuppBagName).get.anno.values.head)
    assert(!aggregatedBag.result.rel.anno.values.contains("ps_supplycost"))
  }

  test("q10") {
    val query =
      """
        |SELECT
        |  c_custkey,
        |  c_name,
        |  sum(l_extendedprice * (1 - l_discount)) AS revenue,
        |  c_acctbal,
        |  n_name,
        |  c_address,
        |  c_phone,
        |  c_comment
        |FROM
        |  customer,
        |  orders, lineitem, nation
        |WHERE
        |  c_custkey = o_custkey
        |  AND l_orderkey = o_orderkey
        |  AND o_orderdate >= DATE '1993-10-01'
        |  AND o_orderdate < DATE '1993-10-01' + INTERVAL '3' MONTH AND l_returnflag = 'R'
        |AND c_nationkey = n_nationkey
        |GROUP BY c_custkey,
        |c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        |ORDER BY
        |revenue DESC;
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(2.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // lineitem is in the second bag, and it is joined with orders, which is in the third bag. This means there should
    // be two aggregations. The first is in the lineitem bag, which sums over everything but l_orderkey (used in the
    // next join) and l_returnflag (selected on).
    assert(optim.rules(1).join.rels.map(_.name).contains("lineitem"))
    val lineitemAgg = optim.rules(1).aggregations.values.head
    assertResult("revenue")(lineitemAgg.annotation)
    assertResult(
      relationsMap("lineitem").schema.attributeNames.toSet -- Set("l_orderkey", "l_returnflag")
    )(lineitemAgg.attrs.values.toSet)
    assertResult("l_extendedprice*(1-l_discount)")(
      lineitemAgg.innerExpression.asInstanceOf[LiteralExpr].value
    )

    // There is also an aggregation on the orders bag, which aggregates out all the attrs. in orders except o_custkey,
    // which is output, and o_orderdate, which is selected. In this aggregation we take the annotation from the bag
    // below (the inner expression just references the bag below).
    assert(optim.rules(2).join.rels.map(_.name).contains("orders"))
    val ordersAgg = optim.rules(2).aggregations.values.head
    assertResult("revenue")(ordersAgg.annotation)
    assertResult(
      Set("o_shippriority", "o_year", "o_orderpriority", "l_orderkey_o_orderkey", "o_clerk", "o_orderstatus",
        "o_totalprice", "o_comment")
    )(ordersAgg.attrs.values.toSet)
    assertResult("revenue")(ordersAgg.innerExpression.asInstanceOf[LiteralExpr].value)

    // Check the annotations on the top two bags.
    assert(optim.rules(2).result.rel.anno.values.contains("revenue"))
    assert(optim.rules(3).result.rel.anno.values.contains("revenue"))
  }

  test("q11") {
    // Pulled out the having.
    val query =
      """
        |CREATE TABLE sq AS (
        |  SELECT
        |    ps_partkey,
        |    sum(ps_supplycost * ps_availqty) AS value
        |  FROM
        |    partsupp, supplier, nation
        |  WHERE
        |    ps_suppkey = s_suppkey
        |    AND s_nationkey = n_nationkey
        |    AND n_name = 'GERMANY'
        |  GROUP BY
        |    ps_partkey
        |);
        |
        |CREATE TABLE lower_bound AS (
        |  SELECT sum(value) * 0.0001
        |  FROM sq
        |);
        |
        |CREATE TABLE q11 AS (
        |  SELECT *
        |  FROM sq
        |  WHERE value > (SELECT *
        |                 FROM lower_bound)
        |  ORDER BY value DESC
        |);
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // Expect 5 bags, three from the first query, and then one each for the next two.
    assertResult(5)(optim.rules.length)
    // Check the result and bag on the lower_bound query.
    val bound = optim.rules(3)
    assertResult("lower_bound")(bound.result.rel.name)
    assertResult(0)(bound.result.rel.attrs.values.length)
    assertResult(1)(bound.result.rel.anno.values.length)

    assertResult(1)(bound.aggregations.values.length)
    val agg = bound.aggregations.values.head
    assertResult(List("ps_partkey"))(agg.attrs.values)
    assertResult("AGG*0.0001")(agg.expression)
    assertResult("value")(agg.innerExpression.asInstanceOf[LiteralExpr].value)

    // Check the last query
    val result = Result(Rel("q11", Attributes(List("ps_partkey")), Annotations(List("value"))), isIntermediate = false)
    assertResult(result)(optim.rules.last.result)
    val filters = Filters(List(Selection("value", GT(), SelectionSubquery("lower_bound"))))
    assertResult(filters)(optim.rules.last.filters)
  }

  test("q12") {
    val query =
      """
        |SELECT
        |  l_shipmode,
        |  sum(CASE
        |      WHEN o_orderpriority = '1-URGENT'
        |           OR o_orderpriority = '2-HIGH'
        |        THEN 1
        |      ELSE 0
        |      END) AS high_line_count,
        |  sum(CASE
        |      WHEN o_orderpriority <> '1-URGENT'
        |           AND o_orderpriority <> '2-HIGH'
        |        THEN 1
        |      ELSE 0
        |      END) AS low_line_count
        |FROM
        |  orders,
        |  lineitem
        |WHERE
        |  o_orderkey = l_orderkey
        |  AND l_shipmode IN ('MAIL', 'SHIP') AND l_commitdate < l_receiptdate
        |  AND l_shipdate < l_commitdate
        |  AND l_receiptdate >= DATE '1994-01-01'
        |  AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' YEAR
        |GROUP BY l_shipmode
        |ORDER BY l_shipmode;
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // A two bag GHD should come out
    assertResult(2)(optim.rules.length)
    // The first bag has order in it. This should have the CASE statements and sum them out over the attributes in the
    // order table
    assertResult(Set("low_line_count", "high_line_count"))(optim.rules.head.aggregations.values.map(_.annotation).toSet)
    val orderAggregationAttrs = Attributes(List(
        "o_shippriority", "o_orderdate", "o_custkey", "o_year", "o_orderpriority", "o_clerk", "o_orderstatus",
        "o_totalprice", "o_comment"
      ))
    assert(optim.rules.head.aggregations.values.forall(_.attrs == orderAggregationAttrs))

    // Check the case statements.
    val highLineCase = optim.rules.head.aggregations.values.find(_.annotation == "high_line_count").get
    val lowLineCase = optim.rules.head.aggregations.values.find(_.annotation == "low_line_count").get
    highLineCase.innerExpression match {
      case CaseExpr(condition, ifCase, elseCase, _, _) => {
        assertResult(1)(condition.length)
        val condition1 = Filters(List(
          Selection("o_orderpriority", EQUALS(), SelectionLiteral("\"1-URGENT\""))
        ))
        val condition2 = Filters(List(
          Selection("o_orderpriority", EQUALS(), SelectionLiteral("\"2-HIGH\""))
        ))
        assertResult(condition.head.value)(SelectionOrList(List(condition1, condition2)))
        assertResult(1)(ifCase.toInt)
        assertResult(0)(elseCase.toInt)
      }
    }

    lowLineCase.innerExpression match {
      case CaseExpr(condition, ifCase, elseCase, _, _) => {
        assertResult(2)(condition.length)
        assertResult(List(
          Selection("o_orderpriority", NOTEQUALS(), SelectionLiteral("\"1-URGENT\"")),
          Selection("o_orderpriority", NOTEQUALS(), SelectionLiteral("\"2-HIGH\""))
        ))(condition)
        assertResult(1)(ifCase.toInt)
        assertResult(0)(elseCase.toInt)
      }
    }

    // The second bag has lineitem in it, and shouldn't have case statements in it.
    assertResult(Set("low_line_count", "high_line_count"))(optim.rules(1).aggregations.values.map(_.annotation).toSet)
    val lineitemAggregationAttrs = Attributes(List(
      "o_orderkey_l_orderkey", "l_returnflag", "l_comment", "l_linestatus", "l_shipinstruct", "l_linenumber", "l_partkey", "l_suppkey"
    ))
    assert(optim.rules(1).aggregations.values.forall(_.attrs == lineitemAggregationAttrs))
  }

  test("q13") {
    // Take out the left outer join and count(orderkey), means we don't list customers with no orders. The difference
    // should just be that one row.
    val query =       """
                        |CREATE TABLE sq AS (
                        |  SELECT
                        |    c_custkey,
                        |    count(*) AS c_count
                        |  FROM
                        |    customer, orders
                        |  WHERE c_custkey = o_custkey
                        |        AND o_comment NOT LIKE '%special%requests%'
                        |  GROUP BY c_custkey
                        |);
                        |
                        |CREATE TABLE q13 AS (
                        |  SELECT
                        |    c_count,
                        |    count(*) AS custdist
                        |  FROM sq
                        |  GROUP BY
                        |    c_count
                        |  ORDER BY
                        |    custdist DESC, c_count DESC);
                      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // There should be two bags for the first query and then one for the second.
    assertResult("sq")(optim.rules(1).result.rel.name)
    assertResult("q13")(optim.rules(2).result.rel.name)

    // Customers is in the first bag, and the COUNT should aggregate over everything except c_custkey
    val agg1 = optim.rules(0).aggregations.values.head

    assertResult(relationsMap("customer").schema.attributeNames.toSet -- Set("c_custkey"))(agg1.attrs.values.toSet)
    assertResult("c_count")(agg1.annotation)
    assertResult(SUM())(agg1.operation)
    assertResult("1")(agg1.init)
    assertResult("AGG")(agg1.expression)
    assertResult("")(agg1.innerExpression.asInstanceOf[LiteralExpr].value)

    // Orders is in the second bag. Here we take the annotation from the first bag and then aggregate out everything
    // but o_custkey and o_comment (which is selected).
    val agg2 = optim.rules(1).aggregations.values.head
    assertResult(relationsMap("orders").schema.attributeNames.toSet -- Set("o_custkey", "o_comment"))(agg2.attrs.values.toSet)
    assertResult("c_count")(agg2.annotation)
    assertResult(SUM())(agg2.operation)
    assertResult("1")(agg2.init)
    assertResult("AGG")(agg2.expression)
    assertResult("c_count")(agg2.innerExpression.asInstanceOf[LiteralExpr].value)

    // The last bag should aggregate out c_custkey
    val custdist = optim.rules.last.aggregations.values.head
    assertResult(List("c_custkey"))(custdist.attrs.values)
  }

  test("q14") {
    // Rewrite as different annotations.
    val query =
      """
        |SELECT
        |  100.00 * sum(CASE
        |               WHEN p_type LIKE 'PROMO%'
        |                 THEN l_extendedprice * (1 - l_discount)
        |               ELSE 0
        |               END)                       AS promo_revenue,
        |  sum(l_extendedprice * (1 - l_discount)) AS total_revenue
        |FROM
        |  lineitem,
        |  part
        |WHERE
        |  l_partkey = p_partkey
        |  AND l_shipdate >= DATE '1995-09-01'
        |  AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH;
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // When we put this through the optimizer, expect lineitem in the bottom bag. This means there is one aggregation
    // on the bottom bag (total_revenue), and then two on the top bag (promo_revenue and total_revenue), because we
    // can't do the second one until the top. Not sure if this really makes sense, maybe this just shouldn't go
    // through the optimizer.
    assertResult(List("total_revenue"))(optim.rules.head.aggregations.values.map(_.annotation))
    assertResult(Set("total_revenue", "promo_revenue"))(optim.rules(1).aggregations.values.map(_.annotation).toSet)
  }

  test("q15") {
    val query =
      """
        |CREATE TABLE revenue AS (
        |  SELECT
        |    l_suppkey                               AS supplier_no,
        |    sum(l_extendedprice * (1 - l_discount)) AS total_revenue
        |  FROM
        |    lineitem
        |  WHERE
        |    l_shipdate >= DATE '1996-01-01'
        |    AND l_shipdate < DATE '1996-01-01' + INTERVAL '3' MONTH GROUP BY
        |  l_suppkey);
        |
        |CREATE TABLE max_revenue AS (
        |  SELECT max(total_revenue)
        |  FROM
        |    revenue);
        |
        |CREATE TABLE q15 AS (
        |  SELECT
        |    s_suppkey,
        |    s_name,
        |    s_address,
        |    s_phone,
        |    total_revenue
        |  FROM
        |    supplier,
        |    revenue
        |  WHERE
        |    s_suppkey = supplier_no AND total_revenue = (
        |      SELECT *
        |      FROM max_revenue
        |    )
        |  ORDER BY
        |    s_suppkey);
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // The first query should be its own bag.
    val revenueResult = Result(
      Rel("revenue", Attributes(List("l_suppkey")), Annotations(List("total_revenue"))),
      isIntermediate = false
    )
    assertResult(revenueResult)(optim.rules.head.result)

    val agg = optim.rules.head.aggregations.values.head

    assertResult("total_revenue")(agg.annotation)
    assertResult("long")(agg.datatype)
    assertResult(SUM())(agg.operation)
    assertResult(List("l_returnflag", "l_comment", "l_linestatus", "l_shipmode", "l_shipinstruct", "l_receiptdate",
      "l_linenumber", "l_partkey", "l_commitdate", "l_orderkey"))(agg.attrs.values)
    assertResult("1")(agg.init)
    assertResult("AGG")(agg.expression)
    assertResult(List())(agg.usedScalars)
    assertResult("l_extendedprice*(1-l_discount)")(agg.innerExpression.asInstanceOf[LiteralExpr].value)

    // The second query is also its own bag
    assertResult(1)(optim.rules(1).result.rel.anno.values.length)
    val maxAgg = optim.rules(1).aggregations.values.head
    assertResult(List("supplier_no"))(maxAgg.attrs.values)
    assertResult(MAX())(maxAgg.operation)
    assertResult("total_revenue")(maxAgg.innerExpression.asInstanceOf[LiteralExpr].value)

    // Check that the selection shows up on the last query.
    val selectionSubquery = Selection("total_revenue", EQUALS(), SelectionSubquery("max_revenue"))
    assert(optim.rules.slice(2,5).flatMap(_.filters.values).contains(selectionSubquery))
  }

  test("q16") {
    // Rewrite to pull out sq
    val query =
      """
        |CREATE TABLE sq AS (
        |  SELECT s_suppkey
        |  FROM
        |    supplier
        |  WHERE
        |    s_comment NOT LIKE '%Customer%Complaints%'
        |);
        |
        |CREATE TABLE q16 AS (
        |  SELECT
        |    p_brand,
        |    p_type,
        |    p_size,
        |    count(DISTINCT ps_suppkey) AS supplier_cnt
        |  FROM
        |    partsupp,
        |    part, sq
        |  WHERE
        |    p_partkey = ps_partkey
        |    AND p_brand <> 'Brand#45'
        |    AND p_type NOT LIKE 'MEDIUM POLISHED%'
        |    AND p_size IN (48, 14, 23, 45, 19, 3, 36, 9) AND ps_suppkey = s_suppkey
        |  GROUP BY
        |    p_brand, p_type, p_size
        |  ORDER BY
        |    supplier_cnt DESC,
        |    p_brand, p_type, p_size);
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // Check the COUNT DISTINCT. It should first appear in the second bag, where part and partsupp are joined. Then, on
    // the last bag, it is summed over the rest of the variables.
    val agg1 = optim.rules(1).aggregations.values.head
    assertResult(COUNT_DISTINCT())(agg1.operation)
    assertResult("ps_suppkey_s_suppkey")(agg1.innerExpression.asInstanceOf[LiteralExpr].value)
    val agg2 = optim.rules.last.aggregations.values.head
    assertResult(COUNT_DISTINCT())(agg2.operation)
    assertResult("supplier_cnt")(agg2.innerExpression.asInstanceOf[LiteralExpr].value)
  }

  test("q17") {
    // Another correlated subquery here.
    val query =
      """
        |CREATE TABLE sq AS (
        |  SELECT
        |    p_partkey             AS sq_p_partkey,
        |    0.2 * avg(l_quantity) AS bound
        |  FROM
        |    part,
        |    lineitem
        |  WHERE
        |    l_partkey = p_partkey
        |  GROUP BY p_partkey
        |);
        |
        |CREATE TABLE q17 AS (
        |  SELECT sum(l_extendedprice) / 7.0 AS avg_yearly
        |  FROM
        |    lineitem,
        |    part, sq
        |  WHERE
        |    p_partkey = l_partkey
        |    AND p_brand = 'Brand#23'
        |    AND p_container = 'MED BOX' AND l_quantity < bound
        |    AND p_partkey = sq_p_partkey
        |);
        |
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // In the subquery, the aggregation should be on the bag with lineitem in it, and sum out everything except
    // l_partkey (which is output).
    assert(optim.rules(1).join.rels.map(_.name).contains("lineitem"))
    val sqAgg = optim.rules(1).aggregations.values.head
    assertResult("0.2*AGG")(sqAgg.expression)
    assertResult(AVG())(sqAgg.operation)
    assertResult(relationsMap("lineitem").schema.attributeNames.toSet -- Set("l_partkey"))(sqAgg.attrs.values.toSet)
    assertResult("l_quantity")(sqAgg.innerExpression.asInstanceOf[LiteralExpr].value)

    // In the second query, lineitem is in the first bag, and the query is partially summed out there (all variables
    // in lineitem except l_partkey). Then, in the top bag is is joined with the other relations, and the rest of the
    // variables are summed out.
    val agg1 = optim.rules(2).aggregations.values.head
    assertResult(relationsMap("lineitem").schema.attributeNames.toSet -- Set("l_partkey"))(agg1.attrs.values.toSet)
    assertResult("AGG/7.0")(agg1.expression)
    val agg2 = optim.rules.last.aggregations.values.head
    assertResult(List("p_partkey_l_partkey_sq_p_partkey"))(agg2.attrs.values)
    assertResult("AGG/7.0")(agg2.expression)

  }

  test("q18") {
    // Rewritten.
    val query =
      """
        |CREATE TABLE sq1 AS (
        |  SELECT
        |    l_orderkey,
        |    sum(l_quantity) AS sum_qt
        |  FROM
        |    lineitem
        |  GROUP BY
        |    l_orderkey
        |);
        |
        |CREATE TABLE sq2 AS (
        |  SELECT
        |    l_orderkey AS sq_l_orderkey,
        |    sum_qt
        |  FROM sq1
        |  WHERE sum_qt > 300
        |);
        |
        |CREATE TABLE q18 AS (
        |  SELECT
        |    c_name,
        |    c_custkey,
        |    o_orderkey,
        |    o_orderdate,
        |    o_totalprice,
        |    sum(l_quantity)
        |  FROM
        |    customer,
        |    orders,
        |    lineitem, sq2
        |  WHERE
        |    o_orderkey = sq_l_orderkey
        |    AND c_custkey = o_custkey AND o_orderkey = l_orderkey
        |  GROUP BY c_name,
        |    c_custkey, o_orderkey, o_orderdate, o_totalprice
        |  ORDER BY
        |    o_totalprice DESC,
        |    o_orderdate
        |);
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // First query should be one bag
    val sq1Rel = Rel("sq1", Attributes(List("l_orderkey")), Annotations(List("sum_qt")))
    assertResult(sq1Rel)(optim.rules.head.result.rel)
    val sq1Agg = optim.rules.head.aggregations.values.head
    assertResult("sum_qt")(sq1Agg.annotation)
    assertResult(relationsMap("lineitem").schema.attributeNames.toSet -- Set("l_orderkey"))(sq1Agg.attrs.values.toSet)
    assertResult("l_quantity")(sq1Agg.innerExpression.asInstanceOf[LiteralExpr].value)

    // Second query is also its own bag. Check that selection on the annotation goes through.
    assertResult(List(sq1Rel))(optim.rules(1).join.rels)
    assertResult(Filters(List(Selection("sum_qt", GT(), SelectionLiteral("300")))))(optim.rules(1).filters)
  }

  test("q19") {
    val query =       """
                        |SELECT sum(l_extendedprice * (1 - l_discount)) AS revenue
                        |FROM
                        |  lineitem,
                        |  part
                        |WHERE
                        |  p_partkey = l_partkey AND (
                        |    (
                        |      p_brand = 'Brand#12'
                        |      AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
                        |      AND l_quantity >= 1 AND l_quantity <= 1 + 10
                        |      AND p_size BETWEEN 1 AND 5
                        |      AND l_shipmode IN ('AIR', 'AIR REG')
                        |      AND l_shipinstruct = 'DELIVER IN PERSON'
                        |    ) OR (
                        |      p_brand = 'Brand#23'
                        |      AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
                        |      AND l_quantity >= 10 AND l_quantity <= 10 + 10
                        |      AND p_size BETWEEN 1 AND 10
                        |      AND l_shipmode IN ('AIR', 'AIR REG')
                        |      AND l_shipinstruct = 'DELIVER IN PERSON'
                        |    ) OR (
                        |      p_brand = 'Brand#34'
                        |      AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
                        |      AND l_quantity >= 20 AND l_quantity <= 20 + 10
                        |      AND p_size BETWEEN 1 AND 15
                        |      AND l_shipmode IN ('AIR', 'AIR REG')
                        |      AND l_shipinstruct = 'DELIVER IN PERSON'
                        |    )
                        |  );
                        |
                      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    // part and lineitem need to be in the same bag because they are tied together by the selection, so the FHW is 2.0
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(2.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)

    // This needs to be one bag, because the selection touches part and lineitem.
    assertResult(1)(optim.rules.length)
    assertResult(Set("lineitem", "part"))(optim.rules.head.join.rels.map(_.name).toSet)

    // Check that the filter goes through
    assertResult(1)(optim.rules.head.filters.values.length)
    val orSelection = optim.rules.head.filters.values.head
    assertResult(OR())(orSelection.operation)
    orSelection.value match {
      case SelectionOrList(filtersList) => {
        assertResult(3)(filtersList.length)
        filtersList.foreach(filters => {
          val involvedAttrs = Set("p_brand", "p_container", "l_quantity", "p_size", "l_shipmode", "l_shipinstruct")
          assertResult(involvedAttrs)(filters.values.map(_.attr).toSet)
        })
      }
    }
  }

  test("q20") {
    val query =
      """
        |CREATE TABLE sq_1 AS (
        |  SELECT p_partkey
        |  FROM
        |    part
        |  WHERE
        |    p_name LIKE 'forest%'
        |);
        |
        |CREATE TABLE sq_2 AS (
        |  SELECT
        |    ps_partkey            AS sq_2_ps_partkey,
        |    ps_suppkey            AS sq_2_ps_suppkey,
        |    0.5 * sum(l_quantity) AS sq_2_availqty
        |  FROM
        |    lineitem, partsupp
        |  WHERE
        |    l_partkey = ps_partkey AND
        |    l_suppkey = ps_suppkey AND
        |    l_shipdate >= DATE '1994-01-01'
        |    AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
        |  GROUP BY
        |  ps_partkey, ps_suppkey
        |);
        |
        |CREATE TABLE sq_3 AS (
        |  SELECT ps_suppkey
        |  FROM
        |    partsupp, sq_2, sq_1
        |  WHERE
        |    ps_partkey = p_partkey
        |    AND ps_availqty > sq_2_availqty
        |    AND ps_partkey = sq_2_ps_partkey
        |    AND ps_suppkey = sq_2_ps_suppkey
        |);
        |
        |CREATE TABLE q20 AS (
        |  SELECT
        |    s_name,
        |    s_address
        |  FROM
        |    supplier, nation, sq_3
        |  WHERE
        |    s_suppkey = ps_suppkey
        |    AND s_nationkey = n_nationkey
        |    AND n_name = 'CANADA'
        |  ORDER BY
        |    s_name);
      """.stripMargin

    val ir = SQLParser.run(query, relationsMap)
    val optim = IROptimizer.setSQLAnnotations(
      IROptimizer.dedupComputations(QueryPlanner.findOptimizedPlans(ir, expectedFHW = Some(1.0)))
    )
    checkJoins(ir.rules)
    checkJoins(optim.rules)
  }

  // TODO: Figure out whether NOT EXISTS can be handled without having a subquery in the query plan.
  test("q21") {
    SQLParser.parseAll(SQLParser.statements,
      """
        |SELECT
        |  s_name,
        |  count(*) AS numwait
        |FROM
        |  supplier, lineitem l1, orders, nation
        |WHERE
        |  s_suppkey = l1.l_suppkey
        |  AND o_orderkey = l1.l_orderkey
        |  AND o_orderstatus = 'F'
        |  AND l1.l_receiptdate > l1.l_commitdate AND exists(
        |      SELECT *
        |      FROM
        |        lineitem l2
        |      WHERE
        |        l2.l_orderkey = l1.l_orderkey
        |        AND l2.l_suppkey <> l1.l_suppkey
        |  )
        |  AND NOT exists(
        |      SELECT *
        |      FROM
        |        lineitem l3
        |      WHERE
        |        l3.l_orderkey = l1.l_orderkey
        |        AND l3.l_suppkey <> l1.l_suppkey
        |        AND l3.l_receiptdate > l3.l_commitdate
        |  )
        |  AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA'
        |GROUP BY s_name
        |ORDER BY
        |  numwait DESC,
        |  s_name;
        |
      """.stripMargin) match {
      case SQLParser.Success(rules, _) =>
      case SQLParser.Failure(_1, _2) => {
        assert(false)
      }
    }
  }

  test("q22") {
    SQLParser.parseAll(SQLParser.statements,
      """
        |SELECT
        |  cntrycode,
        |  count(*)       AS numcust,
        |  sum(c_acctbal) AS totacctbal
        |FROM (
        |       SELECT
        |         substring(c_phone FROM 1 FOR 2) AS cntrycode,
        |         c_acctbal
        |       FROM
        |         customer
        |       WHERE
        |         substring(c_phone FROM 1 FOR 2) IN
        |         ('[I1]', '[I2]', '[I3]', '[I4]', '[I5]', '[I6]', '[I7]') AND c_acctbal > (
        |           SELECT avg(c_acctbal)
        |           FROM
        |             customer
        |           WHERE
        |             c_acctbal > 0.00
        |             AND substring(c_phone FROM 1 FOR 2) IN ('[I1]', '[I2]', '[I3]', '[I4]', '[I5]', '[I6]', '[I7]')
        |         )
        |         AND NOT exists(
        |             SELECT *
        |             FROM orders
        |             WHERE o_custkey = c_custkey
        |         )
        |     ) AS custsale
        |GROUP BY cntrycode
        |ORDER BY cntrycode;
      """.stripMargin) match {
      case SQLParser.Success(rules, _) =>
      case SQLParser.Failure(_1, _2) => {
        assert(false)
      }
    }
  }
}
