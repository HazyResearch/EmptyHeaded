package DunceCap

import DunceCap.attr.Attr
import net.liftweb.json._
import org.scalatest.FunSuite

class QueryPlanTest extends FunSuite {
  test("Can read back in json (note that this isn't a query that makes sense, deleted part of this plan so make test shorter)") {
    val serializedJson =
      """
        |{
        |  "query_type":"join",
        |  "relations":[{
        |    "name":"takesCourse",
        |    "ordering":[0,1],
        |    "annotation":"void*"
        |  },{
        |    "name":"type",
        |    "ordering":[0,1],
        |    "annotation":"void*"
        |  }],
        |  "output":{
        |    "name":"result",
        |    "ordering":[0],
        |    "annotation":"void*"
        |  },
        |  "ghd":[{
        |    "name":"result",
        |    "attributes":["a"],
        |    "annotation":"void*",
        |    "relations":[{
        |      "name":"takesCourse",
        |      "ordering":[0,1],
        |      "attributes":[["a","b"]],
        |      "annotation":"void*"
        |    },{
        |      "name":"bag_1_0",
        |      "ordering":[0],
        |      "attributes":[["a"]],
        |      "annotation":"void*"
        |    }],
        |    "nprr":[{
        |      "name":"a",
        |      "accessors":[{
        |        "name":"takesCourse",
        |        "attrs":["a","b"],
        |        "annotated":false
        |      },{
        |        "name":"bag_1_0",
        |        "attrs":["a"],
        |        "annotated":false
        |      }],
        |      "materialize":true,
        |      "selection":false
        |    }]
        |  }]
        |}
      """.stripMargin
    implicit val formats = DefaultFormats
    val json = parse(serializedJson)
    val plan = json.extract[QueryPlan]
    assertResult(QueryPlan("join", List[QueryPlanRelationInfo](
      QueryPlanRelationInfo("takesCourse", List[Int](0,1), None, "void*"),
      QueryPlanRelationInfo("type", List[Int](0,1), None, "void*")
    ), QueryPlanOutputInfo(
      "result", List[Int](0), "void*"
    ), List[QueryPlanBagInfo](
      QueryPlanBagInfo(
        "result",
        None,
        List[Attr]("a"),
        "void*",
        List[QueryPlanRelationInfo](
          QueryPlanRelationInfo("takesCourse", List[Int](0,1), Some(List[List[Attr]](List[Attr]("a", "b"))), "void*"),
          QueryPlanRelationInfo("bag_1_0", List[Int](0), Some(List[List[Attr]](List[Attr]("a"))), "void*")
        ),
        List[QueryPlanAttrInfo](QueryPlanAttrInfo("a", List[QueryPlanAccessor](
          QueryPlanAccessor("takesCourse", List[Attr]("a", "b"), false),
          QueryPlanAccessor("bag_1_0", List[Attr]("a"), false)
        ), true, false, None, None, None, None)))
    )))(plan)
  }
}
