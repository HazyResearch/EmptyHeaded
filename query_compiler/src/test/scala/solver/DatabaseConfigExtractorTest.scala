package solver

import DunceCap._
import net.liftweb.json._
import org.scalatest.FunSuite

class DatabaseConfigExtractorTest extends FunSuite {
  test("Can properly extract everything from a config file") {
    val serializedJson =
    """
      |{
      |  "layout": "hybrid",
      |  "numNUMA": 4,
      |  "algorithm": "NPRR+",
      |  "database": "\/Users\/caberger\/Documents\/Research\/data\/databases\/higgs\/db_pruned",
      |  "numThreads": 1,
      |  "encodings": {
      |    "node": "long"
      |  },
      |  "relations": {
      |    "R_1_0": "disk",
      |    "R_0_1": "disk"
      |  },
      |  "memory": "ParMemoryBuffer",
      |  "schemas": {
      |    "R": {
      |      "attributes": [
      |        {
      |          "attrType": "long",
      |          "encoding": "node"
      |        },
      |        {
      |          "attrType": "long",
      |          "encoding": "node"
      |        }
      |      ],
      |      "orderings": [
      |        [
      |          0,
      |          1
      |        ],
      |        [
      |          1,
      |          0
      |        ]
      |      ],
      |      "annotation": "void*"
      |    }
      |  }
      |}
    """.stripMargin


    implicit val formats = DefaultFormats
    val json = parse(serializedJson)
    val config = json.extract[DatabaseConfig]

    assertResult(new DatabaseConfig("hybrid", 4, "NPRR+", "/Users/caberger/Documents/Research/data/databases/higgs/db_pruned", 1,
      Map[String,String](("node", "long")), Map[String, String](
        ("R_1_0", "disk"), ("R_0_1", "disk")), "ParMemoryBuffer", Map[String, Schema](
        ("R",
          new Schema(List[Attribute](
            new Attribute("long","node"),
            new Attribute("long","node")), List[List[Int]](List[Int](0,1), List[Int](1,0)), "void*")))
    ))(config)
  }
}
