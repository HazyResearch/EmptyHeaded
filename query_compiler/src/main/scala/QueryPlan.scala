package DunceCap

import DunceCap.attr.Attr
import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import scala.io._
import net.liftweb.json.Serialization.writePretty


object QP {
  def fromJSON(filename:String) : QueryPlans = {
    val fileContents = Source.fromFile(filename).getLines.mkString
    implicit val formats = DefaultFormats
    parse(fileContents).extract[QueryPlans]
  }
}

case class QueryPlans(val queryPlans:List[QueryPlan]) {
  override def toString(): String = {
    implicit val formats = DefaultFormats
    writePretty(this)
  }

  def toJSON(): Unit = {
    val filename = "query.json"
    implicit val formats = Serialization.formats(NoTypeHints)
    scala.tools.nsc.io.File(filename).writeAll(writePretty(this))
  }
}

/**
 * @param query_type either "join" (if nonrecursive) or "recursion"
 * @param relations the input relations for the query
 * @param output the output relation
 * @param ghd the ghd, for the bottom up pass
 * @param topdown information for the topdown pass of yannakakis
 */
case class QueryPlan(val query_type:String,
                val relations:List[QueryPlanRelationInfo],
                val output:QueryPlanOutputInfo,
                val ghd:List[QueryPlanBagInfo],
                val topdown:List[TopDownPassIterator])

/**
 * @param name relation's name
 * @param ordering ordering of attributes,
 *                 for example if your query had (a,b,c) but the attribute ordering is (b,a,c), ordering is (1,0,2)
 * @param attributes attributes,
 *                   for each time the relation appeared in the query, in the order they are entered in the query
 * @param annotation annotation type
 */
case class QueryPlanRelationInfo(val name:String,
                            val ordering:List[Int],
                            /*only filled out at top level of GHD*/
                            val attributes:Option[List[List[Attr]]],
                            val annotation:String)

case class QueryPlanOutputInfo(val name:String,
                          val ordering:List[Int],
                          val annotation:String)

/**
 * @param name name of this bag, you can use any name, but our planner would use something like bag_0_a_b_c,
 *             where the 0 means this is the first bag we visit, in a post-order traversal, and the letters are the attributes
 * @param duplicateOf name of bag that this bag is a duplicate of, if any
 */
case class QueryPlanBagInfo(val name:String,
                            val duplicateOf:Option[String],
                            val attributes:List[Attr],
                            val annotation:String,
                            val relations:List[QueryPlanRelationInfo],
                            val nprr:List[QueryPlanAttrInfo],
                            val recursion:Option[QueryPlanRecursion])

/**
 * @param accessors relations that this attr appears in
 * @param annotation if last materialized, the next aggregated attr (may be None), otherwise None
 */
case class QueryPlanAttrInfo(val name:String,
                        val accessors:List[QueryPlanAccessor],
                        val materialize:Boolean,
                        val selection:List[QueryPlanSelection],
                        val annotation:Option[Attr],
                        val aggregation:Option[QueryPlanAggregation],
                        /* The last two here are never filled out in the top down pass*/
                        val prevMaterialized:Option[Attr],
                        val nextMaterialized:Option[Attr])

/**
 * @param input bag name
 * @param criteria epsilon or iterations
 * @param converganceValue a float (if epsilon) or int (if iterations)
 */
case class QueryPlanRecursion(val input:String,
                              val criteria:String,
                              val converganceValue:String)

/**
 * @param operation SUM, COUNT, etc.
 * @param init what to initialize to when there isn't already an annotation
 * @param expression parts of the expression outside the actual aggregation, may be something like agg + 1, for example
 * @param prev previous attribute to be aggregated in nprr
 * @param next next attribute to be aggregated in nprr
 */
case class QueryPlanAggregation(val operation:String,
                           val init:String,
                           val expression:String,
                           val prev:Option[Attr],
                           val next:Option[Attr])

/**
 * @param operation we only support equality (=) right now
 * @param expression
 */
case class QueryPlanSelection(val operation:String,
                              val expression:String)

case class QueryPlanAccessor(val name:String,
                        val attrs:List[Attr],
                        val annotated:Boolean)

/**
 * @param iterator bag name
 * @param attributeInfo
 */
case class TopDownPassIterator(val iterator:String,
                               val attributeInfo:List[QueryPlanAttrInfo])
