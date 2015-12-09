package parser

import DunceCap.{OutputAttributeNotFoundInJoinException, QueryRelation, ASTQueryStatement, Environment}
import org.scalatest.FunSuite

class TypecheckTest extends FunSuite {
  test("Throws exception when output attribute is not in the query body") {
    Environment.fromJSON("example_config/config.json") // load edge relation
    val query = ASTQueryStatement(
        QueryRelation("lhs", List(("a", "", ""), ("d", "", ""))),
        None,
        "join",
        List(QueryRelation("Edge", List(("a", "", ""), ("b", "=", "1"))), QueryRelation("Edge", List(("a", "", ""), ("c", "=", "1")))),
        Map()
      )
    intercept[OutputAttributeNotFoundInJoinException] {
      query.typecheck()
    }
  }

  test("Throws exception when you're attempting to join two cols of different types") {

  }
}
