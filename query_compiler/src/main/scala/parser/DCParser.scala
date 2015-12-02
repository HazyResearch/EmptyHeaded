package DunceCap

import DunceCap.attr.{AttrInfo, Attr, SelectionVal, SelectionOp}

import scala.collection.immutable.List
import scala.util.parsing.combinator.RegexParsers

package object attr {
  type Attr = String
  type SelectionOp = String
  type SelectionVal = String
  type AttrInfo = (Attr, SelectionOp, SelectionVal)
  type AggInfo = Map[String, ParsedAggregate]
}

case class QueryRelation(var name:String, val attrs:List[AttrInfo],  var annotationType:String = "void*") {
  val attrNames = attrs.map(x => x._1)
  override def equals(that: Any): Boolean =
    that match {
      case that: QueryRelation => that.attrs.equals(attrs) && that.name.equals(name) && that.annotationType.equals(annotationType)
      case _ => false
    }

  def printData() = {
    println("name: " + name + " attrs: " + attrs + " annotationType: " + annotationType)
  }
}

class RecursionStatement(val functionName:String, val inputArgument:QueryRelation, val convergance:ConverganceCriteria)

class TransitiveClosureStatement(val join:List[QueryRelation])

case class ParsedAggregate(val op:String,
                           val expressionLeft:String,
                           val init:String,
                           val expressionRight:String) {
  val expression = if (expressionRight.isEmpty) expressionLeft  else expressionLeft + "agg" + expressionRight
}

case class ConverganceCriteria(val converganceType:String, val converganceOp:String, val converganceCondition:String)


class AggregateExpression(val op:String,
                          val attrs:List[String],
                          val init:String){

  def printData() = {
    println("op: " + op)
    println("attrs: " + attrs)
    println("init: " + init)
  }
}
class AnnotationExpression(val boundVariable:String,
                           val leftHalfOfExpr:String,
                           val agg:Option[AggregateExpression],
                           val rightHalfOfExpr:String) {

  def printData() = {
    println("Bound Variable: " + boundVariable + " expr: " + leftHalfOfExpr + rightHalfOfExpr)
    println("agg: ")
    agg.map(_.printData)
  }
}

object DCParser extends RegexParsers {
  def run(line:String, config:Config) : List[QueryPlan] = {
    this.parseAll(this.statements, line) match {
      case DCParser.Success(parsedStatements, _) => {
        Environment.startScope()
        parsedStatements.foreach(parsedStatement => Environment.addRelation(parsedStatement.lhs))
        println(parsedStatements)
        val graph = EvalGraph(parsedStatements)
        val plans = graph.computePlan(config)
        Environment.endScope()
        return plans
      }
    }
  }

  //some basic expressions
  def identifierName:Parser[String] = """[_\p{L}][_\p{L}\p{Nd}]*""".r
  def selectionElement:Parser[String] = """'[^']*'|\d+""".r
  def convergenceCriteria:Parser[String] = """i|c""".r
  def convergenceOp:Parser[String] = """=|<=|<|>|>=""".r
  def numericalValue:Parser[String] = """\d+\.?\d*""".r
  def convergenceExpression:Parser[ASTConvergenceCondition] = "*[" ~> convergenceCriteria ~ convergenceOp ~ numericalValue <~ "]" ^^ {
    case cc~co~cv => {
      if (cc == "i") {
        ASTItersCondition(cv.toInt)
      } else {
        ASTEpsilonCondition(cv.toDouble)
      }
    }
  }

  def selectionOp:Parser[String] = """=""".r
  def typename:Parser[String] = """long|string|float|int""".r
  def emptyStatement = "".r ^^ {case r => List()}
  def emptyQueryRelation = "".r ^^ {case r => new QueryRelation("",List())}
  def emptyString = "".r ^^ {case r => ""}
  def emptyAggType = "".r ^^ {case r => ""}

  //for the lhs expression
  def lhsStatement: Parser[(QueryRelation, Option[ASTConvergenceCondition])] = identifierName ~ ("(" ~> attrList) ~ (aggStatement <~ ")") ~ opt(convergenceExpression)  ^^ {case id~attrNames~(agg~t)~convergence =>
    val attrInfo = attrNames.map(attrName => {(attrName,"","")})
    if((agg,t) != ("",""))
      (new QueryRelation(id, attrInfo, t), convergence)
    else{
      (new QueryRelation(id, attrInfo), convergence)
    }
  }
  def aggStatement = ((";" ~> identifierName) ~ (":" ~> typename)) | (emptyString~emptyAggType)
  def attrList : Parser[List[String]] = notLastAttr | lastAttr | emptyStatement
  def notLastAttr = identifierName ~ ("," ~> attrList) ^^ {case a~rest => a +: rest}
  def lastAttr = identifierName ^^ {case a => List(a)}

  //for the join query
  def joinAndRecursionStatements = (joinStatement | emptyStatement) ^^ {case a => a}


  def joinStatement:Parser[List[QueryRelation]] = multipleJoinIdentifiers | singleJoinIdentifier
  def multipleJoinIdentifiers = (singleJoinIdentifier <~ ",") ~ joinStatement ^^ {case t~rest => t ++: rest}
  def singleJoinIdentifier = identifierName ~ (("(" ~> joinAttrList) <~ ")") ^^ {case id~attrs => List( new QueryRelation(id, attrs) )}
  def joinAttrList : Parser[List[(String,String,String)]] = notLastJoinAttr | lastJoinAttr
  def notLastJoinAttr = selectionStatement ~ ("," ~> joinAttrList) ^^ {case a~rest => a +: rest}
  def lastJoinAttr = selectionStatement ^^ {case a => List(a)}
  def selectionStatement : Parser[(String,String,String)] =  selection | emptySelection
  def selection: Parser[(String,String,String)] = (identifierName ~ selectionOp ~ selectionElement) ^^ {case a~b~c => (a,b,c.replace('\'','\"')) }
  def emptySelection: Parser[(String,String,String)] = identifierName ^^ {case a => (a,"","")}

  //returns the bound annotation mapped to the expression with a annotation in the middle
  //the annotation is (operation,attrs,init)
  def emptyAggregate:Parser[AggregateExpression] = "".r ^^ {case r => new AggregateExpression("",List(),"")}
  def emptyAnnotationMap:Parser[AnnotationExpression] = emptyAggregate ^^ {case r => new AnnotationExpression("","",Some(r),"")}
  def annotationStatement:Parser[AnnotationExpression] = annotation | emptyAnnotationMap

  def expression:Parser[String] = """[^<^>^\]^\[]*""".r
  def annotation:Parser[AnnotationExpression] = (";" ~> identifierName <~ "=") ~ ("[" ~> expression) ~ opt(aggregateStatement) ~ (expression <~ "]") ^^ { // TODO:right side
    case a~b~c~d => new AnnotationExpression(a,b,c,d)
  }
  def aggInit:Parser[String] = (";" ~> numericalValue) | emptyString ^^ { case a => a}
  def aggOp:Parser[String] = """SUM|COUNT|MIN|CONST""".r
  def aggregateStatement = "<<" ~> aggOp ~ ("(" ~> (findStar | attrList)) ~ (aggInit <~ ")") <~ ">>" ^^ {case a~b~c => new AggregateExpression(a,b,c) }
  def findStar = """\*""".r ^^ {case a => List[String](a)}

  def joinType:Parser[String] = """\+""".r
  def emptyJoinType = "".r ^^ {case r => """*"""}
  def joinTypeStatement:Parser[String] =  (("join" ~> "=" ~> joinType <~ ";") | emptyJoinType) ^^ {case a => a}
  def joinAndAnnotationStatement = joinAndRecursionStatements ~ annotationStatement ^^ { case a~b =>
    val joins = a
    val boundVariable = b.boundVariable
    var aggregatesIn:Map[String, ParsedAggregate] = null
    
    val aggregate = b.agg.get
    val operation = if (aggregate.op == "COUNT") "SUM" else aggregate.op
    val init = if (aggregate.op == "COUNT") "1" else aggregate.init
    val attrs = if (aggregate.attrs == List[String]("*")) a.flatMap(_.attrNames).distinct else aggregate.attrs
    aggregatesIn = attrs.map(attr => ((attr -> new ParsedAggregate(operation, b.leftHalfOfExpr, init, b.rightHalfOfExpr)))).toMap

    (a,aggregatesIn)
  }


  def queryStatement:Parser[ASTQueryStatement] = (lhsStatement ~ (":-" ~> joinTypeStatement) ~  joinAndAnnotationStatement <~ ".") ^^ {case a~b~c =>
    new ASTQueryStatement(a._1, a._2, b,c._1,c._2)
  }

  def statement = queryStatement
  def statements = rep1(statement)
}