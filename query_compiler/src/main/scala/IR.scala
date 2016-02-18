package duncecap

import scala.collection.mutable.ListBuffer

case class Attributes(val values:List[String])

case class Annotations(val values:List[String])

case class Rel(
  val name:String,
  val attrs:Attributes,
  val anno:Annotations) {

  def getName():String = { name }

  def getAttributes():Array[String] = { attrs.values.toArray }

  def getAnnotations():Array[String] = { anno.values.toArray }
}

case class Result(val rel:Rel){
  def getRel():Rel = { rel }
}

case class Order(val attrs:Attributes){
  def getAttributes():Array[String] = { attrs.values.toArray }
}

case class Project(val attrs:Attributes){
  def getAttributes():Array[String] = { attrs.values.toArray }
}

case class Operation(val operation:String){
  def getOperation():String = { operation }
}

case class Join(val rels:List[Rel]){
  def getNumRels():Int = { rels.length }
  def getRel(i:Int):Rel = { rels(i) }
}

case class Selection(val attr:String, 
  val operation:SelectionOp,
  val value:String) {
  def getAttr():String = { attr }
  def getOperation():String = { operation.value }
  def getValue():String = { value }
}

abstract class SelectionOp {
  val value:String = ""
}

//FIXME: ADD MORE OPS
case class EQUALS() extends SelectionOp {
  override val value = "="
}

case class Filters(val selections:List[Selection]) {
  def getNumFilters():Int = { selections.length }
  def getSelect(i:Int):Selection = { selections(i) }
}

abstract class AggOp {
  val value:String = ""
}

//FIXME: ADD MORE OPS
case class SUM() extends AggOp {
  override val value = "+"
}

case class Aggregation(
  val operation:AggOp, 
  val attrs:Attributes,
  val init:String,
  val expression:String)

case class Aggregations(val aggregations:List[Aggregation]) {
  def getNumAggregations():Int = {  aggregations.length }
  def getOperation(i:Int):String = { aggregations(i).operation.value }
  def getAttributes(i:Int):Array[String] = { aggregations(i).attrs.values.toArray }
  def getInit(i:Int):String = { aggregations(i).init }
  def getExpression(i:Int):String = { aggregations(i).expression }
}

case class Rule(
  val result:Result,
  val order:Order,
  val project:Project,
  val operation:Operation,
  val join:Join,
  val aggregations:Aggregations,
  val filters:Filters
) {
  def getResult():Result = { result }
  def getOrder():Order = { order }
  def getProject():Project = { project }
  def getOperation():Operation = { operation }
  def getJoin():Join = { join }
  def getFilters():Filters = { filters }
  def getAggregations():Aggregations = { aggregations }
}

case class IR(val statements:List[Rule]) {
  def this(){this(List())}
  def getNumRules():Int = { statements.length }
  def getRule(i:Int):Rule = { statements(i) }
}

///////////////////////////////////////////////////////////
//From here down is just helper methods to bind to python.
//Should not need to use unless you are modifying the python
//jpype bindings.
///////////////////////////////////////////////////////////
object IR {
  //Special builder to convert arrays to lists & check
  //that everything is consistent with what is in DB  
  def buildRel(name:String,
               attrs:Array[String],
               anno:Array[String]) : Rel = {
    return Rel(name,
      Attributes(attrs.toList),
      Annotations(anno.toList))
  }
  
  def buildAttributes(attrs:Array[String]): Attributes = {
    return Attributes(attrs.toList)
  }
}

class IRBuilder() {
  val rules = ListBuffer[Rule]()

  def addRule(rule:Rule) {
    rules += rule
  }

  def build():IR = {
    IR(rules.toList)
  }
}

class AggregationsBuilder() {
  val aggregations = ListBuffer[Aggregation]()

  private def getOp(op:String) : AggOp = {
    op match {
      case "SUM" => SUM()
      case _ =>
        throw new Exception("Aggregation operation " + op + " not supported.")
    }
  }

  def addAggregation(
    operation:String,
    attrs:Array[String],
    init:String,
    expression:String) {
    aggregations += Aggregation(
      getOp(operation),
      Attributes(attrs.toList),
      init,
      expression)
  }

  def build():Aggregations = {
    Aggregations(aggregations.toList)
  }
}

class FilterBuilder(){
  val filters = ListBuffer[Selection]()

  private def getOp(op:String) : SelectionOp = {
    op match {
      case "=" => EQUALS()
      case _ =>
        throw new Exception("Selection operation " + op + " not supported.")
    }
  }

  def buildSelection(attr:String,operation:String,value:String):Selection = {
    Selection(attr,getOp(operation),value)
  }

  def addSelection(sel:Selection) {
    filters += sel
  }

  def build():Filters = {
    Filters(filters.toList)
  }
}

class JoinBuilder(){
  val joins = ListBuffer[Rel]()

  def addRel(name:String,attrs:Array[String],anno:Array[String]) {
    joins += Rel(name,Attributes(attrs.toList),Annotations(anno.toList))
  }

  def build():Join ={
    Join(joins.toList)
  }
}
