package DunceCap

import DunceCap.attr.Attr
import argonaut.Json
import argonaut.Json._

abstract trait JsonSerializable {
  def toJson: Json
}

class QueryPlan(val query_type:String,
                val relations:List[QueryPlanRelationInfo],
                val output:QueryPlanOutputInfo,
                val ghd:List[QueryPlanBagInfo]) extends JsonSerializable {
  override def toJson: Json = {
    Json(
      "query_type" -> jString(query_type),
      "relations" -> jArray(relations.map(_.toJson)),
      "output" -> output.toJson,
      "ghd" -> jArray(ghd.map(_.toJson))
    )
  }
}

class QueryPlanRelationInfo(val name:String,
                            val ordering:List[Int],
                            val attributes:Option[List[List[Attr]]],
                            val annotation:String) extends JsonSerializable {
  override def toJson: Json = {
    if (attributes.isEmpty) {
      Json(
        "name" -> jString(name),
        "ordering" -> jArray(ordering.map(o => jNumber(o))),
        "annotation" -> jString(annotation)
      )
    } else {
      Json(
        "name" -> jString(name),
        "ordering" -> jArray(ordering.map(o => jNumber(o))),
        "attributes" -> jArray(attributes.get.map(attrsPerRel => {
          jArray(attrsPerRel.map(a => jString(a)))
        })),
        "annotation" -> jString(annotation)
      )
    }
  }
}

class QueryPlanOutputInfo(val name:String,
                          val ordering:List[Int],
                          val annotation:String) extends JsonSerializable {
  override def toJson: Json = {
    Json(
      "name" -> jString(name),
      "ordering" -> jArray(ordering.map(o => jNumber(o))),
      "annotation" -> jString(annotation)
    )
  }
}

class QueryPlanBagInfo(val name:String,
                       val attributes:List[Attr],
                       val annotation:String,
                       val relations:List[QueryPlanRelationInfo],
                       val nprr:List[QueryPlanNPRRInfo]) extends JsonSerializable {
  override def toJson: Json = {
    Json(
      "name" -> jString(name),
      "attributes" -> jArray(attributes.map(a => jString(a))),
      "annotation" -> jString(annotation),
      "relations" -> jArray(relations.map(_.toJson)),
      "nprr" ->  jArray(nprr.map(_.toJson))
    )
  }
}

class QueryPlanNPRRInfo(val name:String,
                        val accessors:List[QueryPlanAccessor],
                        val materialize:Boolean,
                        val selection:Boolean,
                        val annotation:Option[Attr],
                        val aggregation:Option[QueryPlanAggregation],
                        val prevMaterialized:Option[Attr],
                        val nextMaterialized:Option[Attr]) extends JsonSerializable {


  override def toJson: Json = {
    Json(
      "name" -> jString(name),
      "accessors" -> jArray(accessors.map(_.toJson)),
      "materialize" -> jBool(materialize),
      "selection" -> jBool(selection),
      "annotation" -> jString(annotation.getOrElse("None")),
      "aggregation" -> aggregation.map(_.toJson).getOrElse(jString("None")),
      "prevMaterialized" -> jString(prevMaterialized.getOrElse("None")),
      "nextMaterialized" -> jString(prevMaterialized.getOrElse("None"))
    )
  }
}

class QueryPlanAggregation(val operation:String,
                           val init:String,
                           val expression:String,
                           val prev:Option[Attr],
                           val next:Option[Attr]) extends JsonSerializable {
  override def toJson: Json = {
    Json(
      "operation" -> jString(operation),
      "init" -> jString(init),
      "expression" -> jString(expression),
      "prev" -> jString(prev.getOrElse("None")),
      "next" -> jString(next.getOrElse("None"))
    )
  }
}

class QueryPlanAccessor(val name:String,
                        val attrs:List[Attr],
                        val annotated:Boolean) extends JsonSerializable {
  override def toJson: Json = {
    Json(
      "name" -> jString(name),
      "attrs" -> jArray(attrs.map(a => jString(a))),
      "annotated" -> jBool(annotated)
    )
  }
}
