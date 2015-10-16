package DunceCap

import DunceCap.attr.Attr
import argonaut.Json

abstract trait JsonSerializable {
  def toJson: Json
  def fromJson: JsonSerializable
}

class QueryPlan(val query_type:String,
                val relations:QueryPlanRelationInfo,
                val output:QueryPlanOutputInfo,
                val ghd:List[QueryPlanBagInfo]) extends JsonSerializable {
  override def toJson: Json = ???

  override def fromJson: JsonSerializable = ???
}

class QueryPlanRelationInfo(val name:String,
                            val ordering:List[Int],
                            val attributes:Option[List[Attr]],
                            val annotation:String) extends JsonSerializable {
  override def toJson: Json = ???
  override def fromJson: JsonSerializable = ???
}

class QueryPlanOutputInfo

class QueryPlanBagInfo
