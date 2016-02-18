package duncecap

import scala.util.parsing.json._
import scala.io._
import scala.util.parsing.combinator.RegexParsers

package object attr {
  type Attr = String
  type AttrType = String
  type SelectionOp = String
  type SelectionVal = String
  type AttrInfo = (Attr, SelectionOp, SelectionVal)
  type AggInfo = Map[String, ParsedAggregate]
  type AnnotationType = String
}

/*
 A datalog parser which parsers EmptyHeaded datalog
 into our intermediate representation (IR.scala).
*/
object DatalogParser extends RegexParsers {
  def run(line:String) : Unit = {
    val ir = new IR()
    this.parseAll(this.statements, line) match {
      case DatalogParser.Success(parsedStatements, _) => {

      } case _ => 
        throw new ParserFailureException()
    }
  }
  def queryStatement = "".r ^^ {case r => """*"""}
  def statements = rep1(queryStatement)
}

case class ParserFailureException() extends Exception(s"""ParserFailureException""")