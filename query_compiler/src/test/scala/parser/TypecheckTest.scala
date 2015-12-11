package parser

import DunceCap.{OutputAttributeNotFoundInJoinException, QueryRelation, ASTQueryStatement, Environment}
import org.scalatest.FunSuite

class TypecheckTest extends FunSuite {
  test("Throws exception when output attribute is not in the query body") {
    Environment.fromJSON("example_config/config.json") // load edge relation
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
        |        }
        |    }
        |}
      """.stripMargin
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
