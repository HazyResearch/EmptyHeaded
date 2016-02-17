package duncecap

import scala.util.parsing.json._
import scala.io._
import scala.util.parsing.combinator.RegexParsers

case class ParserFailureException() extends Exception(s"""ParserFailureException""")

/*
 A datalog parser which parsers EmptyHeaded datalog
 into our intermediate representation (IR.scala).
*/
object DatalogParser extends RegexParsers {
  def run(line:String) : IR = {
    val irbuilder = new IRBuilder()
    this.parseAll(this.rule, line) match {
      case DatalogParser.Success(parsedRules, _) => {
        irbuilder.addRules(parsedRules)
      } case _ => 
        throw new ParserFailureException()
    }
    irbuilder.build()
  }

  def emptyStatement = "".r ^^ {case r => List()}
  def emptyString = "".r ^^ {case r => ""}
  def identifierName:Parser[String] = """[_\p{L}][_\p{L}\p{Nd}]*""".r
  def attrList : Parser[List[String]] = notLastAttr | lastAttr | emptyStatement
  def notLastAttr = identifierName ~ ("," ~> attrList) ^^ {case a~rest => a +: rest}
  def lastAttr = identifierName ^^ {case a => List(a)}
  def aggStatement = ((";" ~> attrList) | emptyStatement)

  def result: Parser[Result] = identifierName ~ ("(" ~> attrList) ~ (aggStatement <~ ")") ^^ {case id~attrNames~annoNames =>
   Result(Rel(id,Attributes(attrNames),Annotations(annoNames)))
  }

  def convergenceCriteria:Parser[String] = """i|c""".r
  def convergenceOp:Parser[String] = """=|<=|<|>|>=""".r
  def numericalValue:Parser[String] = """\d+\.?\d*""".r
  def convergenceExpression:Parser[Recursion] = "*[" ~> convergenceCriteria ~ convergenceOp ~ numericalValue <~ "]" ^^ {
    case cc~co~cv => {
      val criteria = cc match {
        case "i" => ITERATIONS()
        case _ => throw new Exception("Convergance criteria "+cc+" not supported.")
      }
      val operation = co match {
        case "=" => EQUALS()
        case _ => throw new Exception("Convergance operation "+co+" not supported.")
      }
      Recursion(criteria,operation,cv)
    }
  }

  def datalogRule:Parser[Rule] = result ~ opt(convergenceExpression) ^^ {
    case rslt~ce => {
      val relR = new Rel("R",Attributes(List("a","b")),Annotations(List()))
      
      val order = Order(Attributes(List("a","b")))
      val recursion = None
      val project = Project(Attributes(List()))
      val operation = Operation("*")
      val join = Join(List(relR))
      val aggregations = Aggregations(List(new Aggregation(new SUM(),Attributes(List("a")),"1","")))
      val filters = Filters(List(new Selection("a",new EQUALS(),"1.0")))
      println(rslt)
      println(ce)
      new Rule(rslt,recursion,order,project,operation,join,aggregations,filters)
    }
  }

  def notLastRule:Parser[List[Rule]] = datalogRule ~ ("." ~> rule) ^^ {case a~rest => a +: rest}
  def lastRule:Parser[List[Rule]] = datalogRule ^^ {case a => List(a)}
  def rule:Parser[List[Rule]] = notLastRule | lastRule | emptyStatement

  def rules = rep1(rule)
}
