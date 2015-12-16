package parser

import DunceCap._
import org.scalatest.FunSuite

class TypecheckTest extends FunSuite {
  val configContents =
    """
      |{
      |    "algorithm": "NPRR+",
      |    "database": "/dfs/scratch0/susanctu/newversion/EmptyHeaded/examples/graph/data/facebook/db_pruned",
      |    "encodings": {
      |        "node": "long"
      |    },
      |    "layout": "hybrid",
      |    "memory": "ParMemoryBuffer",
      |    "numNUMA": 4,
      |    "numThreads": 4,
      |    "relations": {
      |        "Edge_0_1": "disk",
      |        "Edge_1_0": "disk"
      |    },
      |    "resultName": "",
      |    "resultOrdering": [],
      |    "schemas": {
      |        "Edge": {
      |            "annotation": "void*",
      |            "attributes": [
      |                {
      |                    "attrType": "long",
      |                    "encoding": "node"
      |                },
      |                {
      |                    "attrType": "long",
      |                    "encoding": "node"
      |                }
      |            ],
      |            "orderings": [
      |                [
      |                    0,
      |                    1
      |                ],
      |                [
      |                    1,
      |                    0
      |                ]
      |            ]
      |        },
      |        "R": {
      |            "annotation": "void*",
      |            "attributes": [
      |                {
      |                    "attrType": "string",
      |                    "encoding": "names"
      |                },
      |                {
      |                    "attrType": "long",
      |                    "encoding": "node"
      |                }
      |            ],
      |            "orderings": [
      |                [
      |                    0,
      |                    1
      |                ],
      |                [
      |                    1,
      |                    0
      |                ]
      |            ]
      |        }
      |    }
      |}
    """.stripMargin

  test("Can have different types in query, as long as you only join cols of the same type") {
    Environment.fromJsonString(configContents)
    val query = ASTQueryStatement(
      QueryRelation("lhs", List(("a", "", ""), ("b", "", ""), ("c", "", ""))),
      None,
      "join",
      List(QueryRelation("Edge", List(("a", "", ""), ("b", "", ""))), QueryRelation("R", List(("c", "", ""), ("a", "", "")))),
      Map()
    )
    query.typecheck()
  }

  test("Throws exception when output attribute is not in the query body") {
    Environment.fromJsonString(configContents)
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
    Environment.fromJsonString(configContents)
    val query = ASTQueryStatement(
      QueryRelation("lhs", List(("a", "", ""), ("b", "", ""), ("c", "", ""))),
      None,
      "join",
      List(QueryRelation("Edge", List(("a", "", ""), ("b", "", ""))), QueryRelation("R", List(("a", "", ""), ("c", "", "")))),
      Map()
    )
    intercept[JoinTypeMismatchException] {
      query.typecheck()
    }
  }
}
