package DunceCap

import DunceCap.attr.{Attr, SelectionVal, SelectionOp}

import scala.collection.immutable.List
import scala.util.parsing.combinator.RegexParsers

package object attr {
  type Attr = String
  type SelectionOp = String
  type SelectionVal = String
}

class QueryRelation(val name:String, val attrs:List[(Attr, SelectionOp, SelectionVal)],  var annotationType:String = "void*") {
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

class ParsedAggregate(val op:String, val expression:String, val init:String)

class ConverganceCriteria(val converganceType:String, val converganceOp:String, val converganceCondition:String)


class AggregateExpression(val op:String,
                          val attrs:List[String],
                          val init:String){

  def printData() = {
    println("op: " + op)
    println("attrs: " + attrs)
    println("init: " + init)
  }
}
class AnnotationExpression( val boundVariable:String,
                            val expr:String,
                            val agg:AggregateExpression) {

  def printData() = {
    println("Bound Variable: " + boundVariable + " expr: " + expr)
    println("agg: ")
    agg.printData()
  }
}

object DCParser extends RegexParsers {
  def run(line:String) : QueryPlan = {
    this.parseAll(this.statements, line) match {
      case DCParser.Success(parsedStatements, _) => {
        parsedStatements.head match {
          case a:ASTQueryStatement =>
            return a.queryPlan
        }
      }
    }
  }

  //some basic expressions
  def identifierName:Parser[String] = """[_\p{L}][_\p{L}\p{Nd}]*""".r
  def selectionElement:Parser[String] = """"[^"]*"|\d+""".r
  def convergenceCriteria:Parser[String] = """i|c""".r
  def convergenceOp:Parser[String] = """=|<=|<|>|>=""".r
  def convergenceCondition:Parser[String] = """\d+\.?\d*""".r

  def selectionOp:Parser[String] = """=""".r
  def typename:Parser[String] = """long|string|float|int""".r
  def emptyStatement = "".r ^^ {case r => List()}
  def emptyQueryRelation = "".r ^^ {case r => new QueryRelation("",List())}
  def emptyString = "".r ^^ {case r => ""}
  def emptyAggType = "".r ^^ {case r => ""}

  //for the lhs expression
  def lhsStatement = relationIdentifier
  def relationIdentifier: Parser[QueryRelation] = identifierName ~ ("(" ~> attrList) ~ (aggStatement <~ ")")  ^^ {case id~attrNames~(agg~t) =>
    val attrInfo = attrNames.map(attrName => {(attrName,"","")})
    if((agg,t) != ("",""))
      new QueryRelation(id, attrInfo, t)
    else{
      new QueryRelation(id, attrInfo)
    }
  }
  def aggStatement = ((";" ~> identifierName) ~ (":" ~> typename)) | (emptyString~emptyAggType)
  def attrList : Parser[List[String]] = notLastAttr | lastAttr | emptyStatement
  def notLastAttr = identifierName ~ ("," ~> attrList) ^^ {case a~rest => a +: rest}
  def lastAttr = identifierName ^^ {case a => List(a)}

  //for the join query
  def joinAndRecursionStatements = (joinStatement | emptyStatement) ~ recursionStatement ^^ {case a~b => (a,b)}
  def recursionStatement = transitiveClosure | recursion | emptyRecursion
  def transitiveClosure = ("[" ~> joinStatement ~ emptyString ~ emptyQueryRelation <~ "]*") ~ emptyString ~ emptyString ~ emptyString ^^ {case a~b~c~d~e~f => (a,b,c,d,e,f)}
  def recursion = ("[" ~> emptyStatement) ~  identifierName ~ ("(" ~> relationIdentifier <~ ")") ~ ("]*" ~>  converganceStatement) ^^ {case a~b~c~(d~e~f) => (a,b,c,d,e,f)}
  def converganceStatement = ( ("""{""" ~> convergenceCriteria) ~ convergenceOp ~ (convergenceCondition <~ """}""") ) | (emptyString~emptyString~emptyString)
  def emptyRecursion = emptyStatement ~ emptyString ~ emptyQueryRelation ~ emptyString ~ emptyString ~ emptyString ^^ {case a~b~c~d~e~f => (a,b,c,d,e,f)}

  def joinStatement:Parser[List[QueryRelation]] = multipleJoinIdentifiers | singleJoinIdentifier
  def multipleJoinIdentifiers = (singleJoinIdentifier <~ ",") ~ joinStatement ^^ {case t~rest => t ++: rest}
  def singleJoinIdentifier = identifierName ~ (("(" ~> joinAttrList) <~ ")") ^^ {case id~attrs => List( new QueryRelation(id, attrs) )}
  def joinAttrList : Parser[List[(String,String,String)]] = notLastJoinAttr | lastJoinAttr
  def notLastJoinAttr = selectionStatement ~ ("," ~> joinAttrList) ^^ {case a~rest => a +: rest}
  def lastJoinAttr = selectionStatement ^^ {case a => List(a)}
  def selectionStatement : Parser[(String,String,String)] =  selection | emptySelection
  def selection: Parser[(String,String,String)] = (identifierName ~ selectionOp ~ selectionElement) ^^ {case a~b~c => (a,b,c) }
  def emptySelection: Parser[(String,String,String)] = identifierName ^^ {case a => (a,"","")}

  //returns the bound annotation mapped to the expression with a annotation in the middle
  //the annotation is (operation,attrs,init)
  def emptyAggregate:Parser[AggregateExpression] = "".r ^^ {case r => new AggregateExpression("",List(),"")}
  def emptyAnnotationMap:Parser[AnnotationExpression] = emptyAggregate ^^ {case r => new AnnotationExpression("","",r)}
  def annotationStatement:Parser[AnnotationExpression] = annotation | emptyAnnotationMap

  def expression:Parser[String] = """[^<]*""".r
  def annotation:Parser[AnnotationExpression] = (";" ~> identifierName <~ "=") ~ expression ~ aggregateStatement ^^ {case a~b~c => new AnnotationExpression(a,b,c)}
  def aggInit:Parser[String] = (";" ~> convergenceCondition) | emptyString ^^ { case a => a}
  def aggOp:Parser[String] = """SUM|COUNT|MIN""".r
  def aggregateStatement = "<" ~> aggOp ~ ("(" ~> (findStar | attrList)) ~ (aggInit <~ ")") <~ ">" ^^ {case a~b~c => new AggregateExpression(a,b,c) }
  def findStar = """\*""".r ^^ {case a => List[String](a)}

  def joinType:Parser[String] = """\+""".r
  def emptyJoinType = "".r ^^ {case r => """*"""}
  def joinTypeStatement:Parser[String] =  (("join" ~> "=" ~> joinType <~ ";") | emptyJoinType) ^^ {case a => a}
  def joinAndAnnotationStatement = joinAndRecursionStatements ~ annotationStatement ^^ { case a~b =>
    val joins = a._1
    val recursion = a._2
    val transitiveClosure = recursion._1
    val recursiveFunction = recursion._2
    val recursiveInputArg = recursion._3
    val convIdentifier = recursion._4
    val convOperator = recursion._5
    val convCriteria = recursion._6

    val boundVariable = b.boundVariable
    val expr = b.expr
    val aggregate = b.agg
    val operation = if(aggregate.op == "COUNT") "SUM" else aggregate.op
    val init = if(aggregate.op == "COUNT") "1" else aggregate.init
    val attrs = if(aggregate.attrs == List[String]("*")) a._1.flatMap(r => r.attrs.map(attr => attr._1)).distinct else aggregate.attrs
    val aggregatesIn = attrs.map(attr => ((attr -> new ParsedAggregate(operation,expr,init)))).toMap

    val recursionIn = if(recursiveFunction != "") Some(new RecursionStatement(recursiveFunction,recursiveInputArg,new ConverganceCriteria(convIdentifier,convOperator,convCriteria))) else None
    val tc = if(transitiveClosure.length != 0) Some(new TransitiveClosureStatement(transitiveClosure))  else None
    (a._1,recursionIn,tc,aggregatesIn)
  }

  def lambdaExpression = ( (identifierName <~ ":-") ~ ("(" ~> lhsStatement <~ "=>") ~ ("{" ~> joinStatement) ~ (annotationStatement <~ "}).") ) ^^ {case a~b~c~d =>
    val boundVariable = d.boundVariable
    val expr = d.expr
    val aggregate = d.agg
    val operation = if(aggregate.op == "COUNT") "SUM" else aggregate.op
    val init = if(aggregate.op == "COUNT") "1" else aggregate.init
    val attrs = if(aggregate.attrs == List[String]("*")) c.flatMap(r => r.attrs.map(attr => attr._1)).distinct else aggregate.attrs
    val aggregatesIn = attrs.map(attr => ((attr -> new ParsedAggregate(operation,expr,init)))).toMap
  }


  def queryStatement = (lhsStatement ~ (":-" ~> joinTypeStatement) ~  joinAndAnnotationStatement <~ ".") ^^ {case a~b~c =>
    new ASTQueryStatement(a,b,c._1,c._2,c._3,c._4)
  }

  def statement = queryStatement
  def statements = rep(statement)
}
