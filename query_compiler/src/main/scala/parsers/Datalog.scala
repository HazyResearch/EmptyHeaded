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
  def op:Parser[String] = """=|<=|<|>|>=""".r
  def numericalValue:Parser[String] = """\d+\.?\d*""".r
  def recursion:Parser[Recursion] = "*[" ~> convergenceCriteria ~ op ~ numericalValue <~ "]" ^^ {
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

  def joinType:Parser[String] = """\+|\*""".r
  def emptyJoinType = "".r ^^ {case r => """*"""}
  def operation:Parser[Operation] =  (("op" ~> "=" ~> joinType <~ ";") | emptyJoinType) ^^ {case a => Operation(a)}

  def join:Parser[List[(Option[Rel],Option[Aggregation],Option[Selection])]] = 
    identifierName ~ ("(" ~> attrList <~ ")") ^^ {case r~a => 
      List((Some(Rel(r,Attributes(a))),None,None))}

  def selectionString:Parser[String] = """'[^']*'|\d+""".r
  def selectionElement:Parser[String] = selectionString | numericalValue
  def selection:Parser[List[(Option[Rel],Option[Aggregation],Option[Selection])]] = 
    identifierName ~ op ~ selectionElement ^^ {case a~b~c => 
      val fb = new FilterBuilder()
      List((None,None,Some(fb.buildSelection(a,b,c))))
    }

  def aggInit:Parser[String] = (";" ~> numericalValue) | emptyString ^^ { case a => a}
  def aggOp:Parser[String] = """SUM|COUNT|MIN|CONST""".r
  def findStar = """\*""".r ^^ {case a => List[String](a)}
  def aggregateStatement = aggOp ~ ("(" ~> (findStar | attrList)) ~ (aggInit <~ ")")

  def aggexpression1:Parser[String] = """[^(SUM|COUNT|MIN|CONST)]*""".r
  def aggexpression2:Parser[String] = s"""[^\\]]*""".r
  def aggregation:Parser[List[(Option[Rel],Option[Aggregation],Option[Selection])]] = 
    (identifierName <~ "<-") ~ ("[" ~> aggexpression1) ~ aggregateStatement ~ (aggexpression2 <~ "]") ^^ {case a~b~(op~attrs~init)~d => 
      val ab = new AggregationsBuilder()
      val agg = Aggregation(a,
        ab.getOp(op),
        Attributes(attrs),
        init,
        b+"$AGG$"+d)
      List((None,Some(agg),None))
    }

  def clause:Parser[List[(Option[Rel],Option[Aggregation],Option[Selection])]] = join | selection | aggregation

  def clauses:Parser[List[(Option[Rel],Option[Aggregation],Option[Selection])]] = notlastclause | clause
  def notlastclause:Parser[List[(Option[Rel],Option[Aggregation],Option[Selection])]] = 
    clause ~ ("," ~> clauses) ^^ {case a~rest => a ++: rest}

  def datalogRule:Parser[Rule] = result ~ opt(recursion) ~ (":-" ~> operation) ~ clauses ^^ {
    case rslt~ce~jt~clse => {
      val joinedRelations = clse.filter(t => t._1 != None).map(_._1.get)
      val aggs = clse.filter(t => t._2 != None).map(_._2.get)
      val selections = clse.filter(t => t._3 != None).map(_._3.get)

      val join = Join(joinedRelations)
      val filters = Filters(selections)

      val attrs = join.rels.flatMap(r => {
        r.attrs.values
      }).distinct.sorted
      
      //check aggregation for *
      //check that annotation in aggregation is an annotation
      //set default init if it isn't set
      val aggs_in = aggs.map(agg => {
        val newAttrs = 
          if(agg.attrs.values.length == 1 && agg.attrs.values(0) == "*") Attributes(attrs)
          else agg.attrs
        if(!rslt.rel.anno.values.contains(agg.annotation))
          throw new Exception("If an aggregation is specified the annotation must appear in the head.")
        val newinit = (agg.init,jt.operation) match {
          case ("","*") => "1"
          case ("","+") => "0"
          case _ => agg.init
        }
        Aggregation(agg.annotation,agg.operation,newAttrs,newinit,agg.expression)
      })

      //a little logic to figure out which attrs are projected
      val annotations = aggs_in.map(agg => agg.annotation)
      val aggAttrs = aggs_in.flatMap(agg => agg.attrs.values)
      val notProjectedAttrs = rslt.rel.attrs.values ++ aggAttrs
      val projectedAttrs = attrs.filter(a => !notProjectedAttrs.contains(a))

      //lets check that annotations in the head appear in the body
      rslt.rel.anno.values.foreach(a => {
        if(!annotations.contains(a))
          throw new Exception("Annotations specified in the head must appear in the body.")
      })
      //lets check that attributes in the head appear in the body
      rslt.rel.attrs.values.foreach(a => {
        if(!attrs.contains(a))
          throw new Exception("Attributes specified in the head must appear in the body.")
      })

      val order = Order(Attributes(attrs)) //just sorted before GHD optimizer
      val project = Project(Attributes(projectedAttrs))
      val aggregations = Aggregations(aggs_in)
      println("\nSTATEMENT")
      println(rslt)
      println(ce)
      println(jt)
      println(order)
      println(project)
      println(join)
      println(aggregations)
      println(filters)
      new Rule(rslt,ce,jt,order,project,join,aggregations,filters)
    }
  }

  def notLastRule:Parser[List[Rule]] = datalogRule ~ ("." ~> rule) ^^ {case a~rest => a +: rest}
  def lastRule:Parser[List[Rule]] = datalogRule ^^ {case a => List(a)}
  def rule:Parser[List[Rule]] = notLastRule | lastRule | emptyStatement

  def rules = rep1(rule)
}
