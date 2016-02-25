///////////////////////////////////////////////////////////////////////////////
// Generates the code that loads and creates the database.
//
///////////////////////////////////////////////////////////////////////////////
package duncecap

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map

import sys.process._
import java.io.{FileWriter, File, BufferedWriter}
import scala.collection.mutable.Set
import java.nio.file.{Paths, Files}
import util.control.Breaks._

import net.liftweb.json.DefaultFormats
import net.liftweb.json._
import scala.io._
import net.liftweb.json.Serialization.writePretty

object QueryPlan{
  //loads the relations from disk (if nesc.)
  //and encodes them, then spills the encodings to disk
  //next builds the tries and spills to disk
  def generate(ir:IR,db:DBInstance){
    //first split the rules apart into those that are connected
    //and those that are not. the dependencies should come in an 
    //ordered fashion in the rules.
    val independentrules = getIndependentRules(ir)
    var i = 0
    independentrules.foreach(rules => {
      //figure out what relations we need
      val rels = ir2relationinfo(rules)
      val ghd = rules.map(rule =>{
        //fixme figure out anno type
        val name = rule.result.rel.name
        val duplicateOf = None
        val attributes = rule.order.attrs
        val annotation = "void*"
        val relations = ir2relationinfo(List(rule))
        val nprr = getattrinfo(rule)
        val recursion = getbagrecursion(rule)

        QueryPlanBagInfo(
          name,
          duplicateOf,
          attributes,
          annotation,
          relations,
          nprr,
          recursion
        )
      }).toList
      val topdown = List(TopDownPassIterator("",List()))
      val myplan = QueryPlan(rels,ghd,topdown)
      EHGenerator.run(myplan,db,i.toString)
      i += 1
    })
  }

  private def getattrinfo(rule:Rule) : List[QueryPlanAttrInfo] = {
    //create accessors for each attribute
    val accessorMap = Map[String,ListBuffer[QueryPlanAccessor]]()
    val selectionMap = Map[String,ListBuffer[QueryPlanSelection]]()
    rule.order.attrs.values.foreach(a => {
      accessorMap += (a -> ListBuffer())
      selectionMap += (a -> ListBuffer())
    })

    //build up accessors
    rule.join.rels.foreach(r => {
      r.attrs.values.foreach(a => {
        accessorMap(a) += QueryPlanAccessor(r.name,r.attrs,false)
      })
    })
    //build up selections
    rule.filters.values.foreach(sel => {
      selectionMap(sel.attr) += QueryPlanSelection(sel.operation.value,sel.value)
    })

    //build up aggreations
    val aggregationMap = Map[String,QueryPlanAggregation]()
    assert(rule.aggregations.values.length <= 1)
    if(rule.aggregations.values.length == 1){
      val agg = rule.aggregations.values.head
      val aggattrs = agg.attrs.values.sortBy(rule.order.attrs.values.indexOf(_))
      aggattrs.foreach(a => {
        val prev = if(aggattrs.indexOf(a) == 0) None else Some(aggattrs(aggattrs.indexOf(a)-1))
        val next = if((aggattrs.indexOf(a)+1) == aggattrs.length) None else Some(aggattrs(aggattrs.indexOf(a)+1))
        aggregationMap += (a -> QueryPlanAggregation(
          agg.operation.value,
          agg.init,
          agg.expression,
          prev,
          next))
      })
    }

    val materializedattrs = rule.order.attrs.values.filter(a => rule.result.rel.attrs.values.contains(a))

    //finally build the attribute info
    rule.order.attrs.values.map(a => {
      val name = a
      val accessors = accessorMap(a).toList
      val materialize = rule.result.rel.attrs.values.contains(a)
      val selection = selectionMap(a).toList
      val annotation = None
      val aggregation = if(aggregationMap.contains(a)) Some(aggregationMap(a)) else None
      val prevMaterialized = if(materializedattrs.indexOf(a) <= 0) None else Some(materializedattrs(materializedattrs.indexOf(a)-1))
      val nextMaterialized = if((materializedattrs.indexOf(a)+1) == materializedattrs.length) None else Some(materializedattrs(materializedattrs.indexOf(a)+1))

      QueryPlanAttrInfo(name,accessors,materialize,selection,annotation,aggregation,prevMaterialized,nextMaterialized)
    }).toList
  }

  private def getbagrecursion(rule:Rule):Option[QueryPlanRecursion] = {
    rule.recursion match {
      case Some(rec) => {
        val input = rec.criteria match {
          case a:ITERATIONS => "i"
          case _ =>
            throw new Exception("not valid recursion")
        }
        Some(QueryPlanRecursion(
          input,
          rec.operation.value,
          rec.value
        ))
      }
      case None => None
    }
  }

  private def ir2relationinfo(rules:List[Rule]) : List[QueryPlanRelationInfo] = {
    val relations = Map[(String,List[Int]),ListBuffer[Attributes]]()
    rules.foreach(rule => {
      val globalorder = rule.order.attrs.values
      rule.join.rels.foreach(rel => {
        val order = (0 until rel.attrs.values.length).
          sortBy(i => globalorder.indexOf(rel.attrs.values(i))).toList
        if(!relations.contains((rel.name,order)))
          relations += ((rel.name,order) -> ListBuffer(rel.attrs))
        else
          relations(((rel.name,order))) += rel.attrs
      })
    })
    //FIX ME Look up relations in DB and get annotation
    relations.map(relMap => {
      QueryPlanRelationInfo(relMap._1._1,relMap._1._2,Some(relMap._2.toList),"void*")
    }).toList
  }

  private def getIndependentRules(ir:IR): List[List[Rule]] = {
    //set of relation names -> rules
    val rules = ListBuffer[ListBuffer[Rule]]()
    val rulenames = ListBuffer[ListBuffer[String]]()
    val rel = Map[String,Int]() //map from relation name to index in rules buffer
    ir.rules.foreach(rule => {
      var rulesindex = -1
      var rulesindex2 = -1
      rule.join.rels.foreach(r => {
        if(rel.contains(r.name)){
          if(rulesindex != -1 && rulesindex != rel(r.name))
            throw new Exception("Multiply dependencies should never occur.")
          rulesindex = rel(r.name)
          if(rules(rulesindex).indexOf(r.name) > rulesindex2)
            rulesindex2 = rules(rulesindex).indexOf(r.name)
        }
      })

      if(rulesindex == -1){
        rel += (rule.result.rel.name -> rules.length)
        rulenames.append(ListBuffer(rule.result.rel.name))
        rules.append(ListBuffer(rule))
      } else{
        rules(rulesindex).append(rule)
        rulenames(rulesindex).append(rule.result.rel.name)
      }
    })
    rules.map(_.toList).toList
  }
}

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
case class QueryPlan(
                val relations:List[QueryPlanRelationInfo],
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
                            val attributes:Option[List[Attributes]],
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
                            val attributes:Attributes,
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
                        val annotation:Option[Attributes],
                        val aggregation:Option[QueryPlanAggregation],
                        /* The last two here are never filled out in the top down pass*/
                        val prevMaterialized:Option[String],
                        val nextMaterialized:Option[String])

/**
 * @param input bag name
 * @param criteria epsilon or iterations
 * @param ConvergenceValue a float (if epsilon) or int (if iterations)
 */
case class QueryPlanRecursion(val input:String,
                              val criteria:String,
                              val ConvergenceValue:String)

/**
 * @param operation SUM, COUNT, etc.
 * @param init what to initialize to when there isn't already an annotation
 * @param expression parts of the expression outside the actual aggregation, may be something like agg+1, for example
 * @param prev previous attribute to be aggregated in nprr
 * @param next next attribute to be aggregated in nprr
 */
case class QueryPlanAggregation(val operation:String,
                           val init:String,
                           val expression:String,
                           val prev:Option[String],
                           val next:Option[String])

/**
 * @param operation we only support equality (=) right now
 * @param expression
 */
case class QueryPlanSelection(val operation:String,
                              val expression:String)

case class QueryPlanAccessor(val name:String,
                        val attrs:Attributes,
                        val annotated:Boolean)

/**
 * @param iterator bag name
 * @param attributeInfo
 */
case class TopDownPassIterator(val iterator:String,
                               val attributeInfo:List[QueryPlanAttrInfo])