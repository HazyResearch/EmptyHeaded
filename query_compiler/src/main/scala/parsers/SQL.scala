package duncecap

import java.util.GregorianCalendar
import scala.language.postfixOps
import scala.util.parsing.combinator.RegexParsers

abstract class SQLExpr {
  def generate():String = {
    val className = this.getClass.getName
    throw new Exception(s""""generate" not implemented by $className""")
  }
}

case class SQLRelation(name: String, alias: Option[String]) extends SQLExpr

case class SQLJoin(relation: SQLRelation, condition: SQLExpr) extends SQLExpr

abstract class SQLAggregation(expr: SQLExpr) extends SQLExpr {
  def getExpr:SQLExpr = expr
}

case class SQLParens(expr: SQLExpr) extends SQLExpr {
  override def generate() = "(" + expr.generate() + ")"
}

case class SQLSum(expr: SQLExpr) extends SQLAggregation(expr)

case class SQLAvg(expr: SQLExpr) extends SQLAggregation(expr)

case class SQLMin(expr: SQLExpr) extends SQLAggregation(expr)

case class SQLMax(expr: SQLExpr) extends SQLAggregation(expr)

case class SQLCount(expr: SQLExpr, distinct: Boolean = false) extends SQLAggregation(expr)

case class SQLInterval(value: Int, units: String) extends SQLExpr {
  override def generate() = {
    value.toString + " " + units
  }
}

case class SQLExists(expr: SQLExpr) extends SQLExpr

case class SQLExtract(col: SQLColumnName, unit: String) extends SQLExpr {
  override def generate() = {
    "EXTRACT(" + col.generate() + "," + unit + ")"
  }
}

case class SQLCase(condition: SQLExpr, ifCase: SQLExpr, elseCase: SQLExpr) extends SQLExpr

case class SQLSubstring(expr: SQLExpr, start: Int, length: Int) extends SQLExpr

abstract class SQLBinaryOperation(lhs: SQLExpr, rhs: SQLExpr) extends SQLExpr {
  val op:String
  override def generate() = {
    lhs.generate() + op + rhs.generate()
  }
  def getLhs() = lhs
  def getRhs() = rhs
}

case class SQLMul(lhs: SQLExpr, rhs: SQLExpr) extends SQLBinaryOperation(lhs, rhs) {
  val op = "*"
}

case class SQLDiv(lhs: SQLExpr, rhs: SQLExpr) extends SQLBinaryOperation(lhs, rhs) {
  val op = "/"
}

case class SQLPlus(lhs: SQLExpr, rhs: SQLExpr) extends SQLBinaryOperation(lhs, rhs) {
  val op = "+"
}

case class SQLMinus(lhs: SQLExpr, rhs: SQLExpr) extends SQLBinaryOperation(lhs, rhs) {
  val op = "-"
}

abstract class SQLBinaryComparison(lhs: SQLExpr, rhs: SQLExpr) extends SQLExpr {
  val op:Op
  def getSelection(columnToJoinedName: Map[SQLColumnName, String]) = {
    val selectionVal = rhs match {
      case d:SQLDate => SelectionDate(d.value)
      case subquery:SQLSelect => {
        subquery.relations match {
          case List(SQLRelation(name, _)) => SelectionSubquery(name)
          case _ => throw new Exception("Subqueries must be scalars.")
        }
      }
      case e => SelectionLiteral(e.generate())
    }
    val lhsName = columnToJoinedName.getOrElse(lhs.asInstanceOf[SQLColumnName], lhs.generate())
    Selection(lhsName, op, selectionVal)
  }
  def getLhs() = lhs
  def getRhs() = rhs
}

case class SQLLTE(lhs: SQLExpr, rhs: SQLExpr) extends SQLBinaryComparison(lhs, rhs) {
  val op = LTE()
}

case class SQLGTE(lhs: SQLExpr, rhs: SQLExpr) extends SQLBinaryComparison(lhs, rhs) {
  val op = GTE()
}

case class SQLLT(lhs: SQLExpr, rhs: SQLExpr) extends SQLBinaryComparison(lhs, rhs) {
  val op = LT()
}

case class SQLGT(lhs: SQLExpr, rhs: SQLExpr) extends SQLBinaryComparison(lhs, rhs) {
  val op = GT()
}

case class SQLEq(lhs: SQLExpr, rhs: SQLExpr) extends SQLBinaryComparison(lhs, rhs) {
  val op = EQUALS()
}

case class SQLNotEq(lhs: SQLExpr, rhs: SQLExpr) extends SQLBinaryComparison(lhs, rhs) {
  val op = NOTEQUALS()
}

case class SQLLike(lhs: SQLExpr, rhs: SQLExpr) extends SQLBinaryComparison(lhs, rhs) {
  val op = LIKE()
}

case class SQLIn(lhs: SQLExpr, exprs: List[SQLExpr]) extends SQLExpr

case class SQLBetween(lhs: SQLExpr, lower: SQLExpr, upper: SQLExpr) extends SQLExpr

case class SQLNot(factor: SQLExpr) extends SQLExpr

case class SQLAnd(lhs: SQLExpr, rhs: SQLExpr) extends SQLExpr

case class SQLOr(exprs: List[SQLExpr]) extends SQLExpr

abstract class SQLAtom extends SQLExpr

case class SQLString(value: String) extends SQLAtom {
  override def generate() = '"' + value + '"'
}

case class SQLNumber(value: String) extends SQLAtom {
  override def generate() = value
}

case class SQLDate(value: java.util.Date) extends SQLAtom

case class SQLColumnName(name: String, tableName: Option[String] = None) extends SQLAtom {
  override def generate() = {
    if (name == "*") ""
    else tableName match {
      case Some(t) => t + "_" + name
      case None => name
    }
  }
}


case class SQLResultColumn(expr: SQLExpr, alias: Option[String], distinct: Boolean = false)

case class SQLOrderingTerm(expr: SQLExpr, desc: Boolean)

case class SQLGroupBy(exprs: List[SQLExpr], having: Option[SQLExpr])

case class SQLSelect(resultColumns: List[SQLResultColumn],
                     relations: List[SQLExpr],
                     joins: Option[List[SQLJoin]],
                     where: Option[SQLExpr],
                     groupBy: Option[SQLGroupBy],
                     orderBy: Option[List[SQLOrderingTerm]],
                     var alias: Option[String]) extends SQLExpr {
  override def generate() = alias.get
}

case class SQLRecursion(baseCase: SQLSelect, recursiveCase: SQLSelect, criteria: Convergence, value: String) extends SQLExpr

object SQLParser extends RegexParsers {

  def caseinsensitive(s: String) = {
    ("""(?i)""" + s).r
  }

  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")

  // Reserved
  def sumKw: Parser[String] = caseinsensitive("sum\\b")

  def avgKw: Parser[String] = caseinsensitive("avg\\b")

  def minKw: Parser[String] = caseinsensitive("min\\b")

  def maxKw: Parser[String] = caseinsensitive("max\\b")

  def countKw: Parser[String] = caseinsensitive("count\\b")

  def intervalKw: Parser[String] = caseinsensitive("interval\\b")

  def daysKw: Parser[String] = caseinsensitive("day\\b")

  def monthKw: Parser[String] = caseinsensitive("month\\b")

  def yearKw: Parser[String] = caseinsensitive("year\\b")

  def asKw: Parser[String] = caseinsensitive("as\\b")

  def descKw: Parser[String] = caseinsensitive("desc\\b")

  def selectKw: Parser[String] = caseinsensitive("select\\b")

  def fromKw: Parser[String] = caseinsensitive("from\\b")

  def whereKw: Parser[String] = caseinsensitive("where\\b")

  def groupByKw: Parser[String] = caseinsensitive("group by\\b")

  def orderByKw: Parser[String] = caseinsensitive("order by\\b")

  def dateKw: Parser[String] = caseinsensitive("date\\b")

  def andKw: Parser[String] = caseinsensitive("and\\b")

  def orKw: Parser[String] = caseinsensitive("or\\b")

  def likeKw: Parser[String] = caseinsensitive("like\\b")

  def existsKw: Parser[String] = caseinsensitive("exists\\b")

  def betweenKw: Parser[String] = caseinsensitive("between\\b")

  def extractKw: Parser[String] = caseinsensitive("extract\\b")

  def caseKw: Parser[String] = caseinsensitive("case\\b")

  def whenKw: Parser[String] = caseinsensitive("when\\b")

  def thenKw: Parser[String] = caseinsensitive("then\\b")

  def elseKw: Parser[String] = caseinsensitive("else\\b")

  def endKw: Parser[String] = caseinsensitive("end\\b")

  def havingKw: Parser[String] = caseinsensitive("having\\b")

  def inKw: Parser[String] = caseinsensitive("in\\b")

  def notKw: Parser[String] = caseinsensitive("not\\b")

  def joinKw: Parser[String] = caseinsensitive("join\\b")

  def onKw: Parser[String] = caseinsensitive("on\\b")

  def distinctKw: Parser[String] = caseinsensitive("distinct\\b")

  def substringKw: Parser[String] = caseinsensitive("substring\\b")

  def forKw: Parser[String] = caseinsensitive("for\\b")

  def createKw: Parser[String] = caseinsensitive("create\\b")

  def viewKw: Parser[String] = caseinsensitive("view\\b")

  def tableKw: Parser[String] = caseinsensitive("table\\b")

  def recursiveKw: Parser[String] = caseinsensitive("recursive\\b")

  def unionKw: Parser[String] = caseinsensitive("union\\b")

  def withKw: Parser[String] = caseinsensitive("with\\b")

  def iterationsKw: Parser[String] = caseinsensitive("iterations\\b")

  def reserved =
    sumKw |
      avgKw |
      minKw |
      maxKw |
      countKw |
      intervalKw |
      daysKw |
      monthKw |
      yearKw |
      asKw |
      descKw |
      selectKw |
      fromKw |
      whereKw |
      groupByKw |
      orderByKw |
      dateKw |
      andKw |
      orKw |
      likeKw |
      existsKw |
      betweenKw |
      extractKw |
      caseKw |
      whenKw |
      thenKw |
      elseKw |
      endKw |
      havingKw |
      inKw |
      notKw |
      joinKw |
      onKw |
      distinctKw |
      substringKw |
      forKw |
      createKw |
      viewKw |
      tableKw |
      recursiveKw |
      unionKw |
      withKw |
      recursiveKw |
      iterationsKw

  def identifierName: Parser[String] = not(reserved) ~> """[a-zA-Z][a-zA-Z0-9_]*""".r

  def relation: Parser[SQLRelation] = identifierName ~ (identifierName ?) ^^ { case t ~ a => SQLRelation(t, a) }

  def relations: Parser[List[SQLExpr]] = repsep(relation | ("(" ~> select <~ ")" <~ asKw <~ identifierName), ",")

  def join: Parser[SQLJoin] = (joinKw ~> relation) ~ (onKw ~> booleanExpr) ^^ {
    case rel ~ condition => SQLJoin(rel, condition)
  }

  def joins: Parser[List[SQLJoin]] = rep(join)

  def number: Parser[SQLNumber] = """\d+(\.\d*)?""".r ^^ { case n => SQLNumber(n) }

  def date: Parser[SQLDate] = dateKw ~> "'" ~> """\d\d\d\d-\d\d-\d\d""".r <~ "'" ^^ {
    case ds => SQLDate(dateFormat.parse(ds))
  }

  def string: Parser[SQLString] = "'" ~> """[^']*""".r <~ "'" ^^ { case s => SQLString(s) }

  def literal: Parser[SQLExpr] = number | date | column | string

  def column: Parser[SQLColumnName] = ((identifierName <~ ".") ?) ~ (identifierName | "*") ^^ {
    case t ~ name => SQLColumnName(name, t)
  }

  def sum: Parser[SQLSum] = sumKw ~> "(" ~> expr <~ ")" ^^ { case e => SQLSum(e) }

  def avg: Parser[SQLAvg] = avgKw ~> "(" ~> expr <~ ")" ^^ { case e => SQLAvg(e) }

  def min: Parser[SQLMin] = minKw ~> "(" ~> expr <~ ")" ^^ { case e => SQLMin(e) }

  def max: Parser[SQLMax] = maxKw ~> "(" ~> expr <~ ")" ^^ { case e => SQLMax(e) }

  def count: Parser[SQLCount] = countKw ~> "(" ~> (distinctKw ?) ~ expr <~ ")" ^^ {
    case d ~ e => SQLCount(e, d.isDefined)
  }

  def interval: Parser[SQLInterval] = (intervalKw ~> "'" ~> """\d+""".r <~ "'") ~ (daysKw | monthKw | yearKw) ^^ {
    case v ~ u => SQLInterval(v.toInt, u)
  }

  def exists: Parser[SQLExpr] = ((notKw ?) <~ existsKw) ~ ("(" ~> expr <~ ")") ^^ {
    case Some(_) ~ e => SQLNot(SQLExists(e))
    case None ~ e => SQLExists(e)
  }

  def extract: Parser[SQLExtract] = (extractKw ~> "(" ~> (daysKw | monthKw | yearKw) <~ fromKw) ~ column <~ ")" ^^ {
    case u ~ c => SQLExtract(c, u)
  }

  def sqlCase: Parser[SQLCase] = (caseKw ~> whenKw ~> booleanExpr) ~ (thenKw ~> expr) ~ (elseKw ~> expr <~ endKw) ^^ {
    case condition ~ ifEx ~ elseEx =>
      SQLCase(condition, ifEx, elseEx)
  }

  def substring: Parser[SQLSubstring] = (substringKw ~> "(" ~> expr <~ fromKw) ~ ("""\d+""".r <~ forKw) ~ ("""\d+""".r <~ ")") ^^ {
    case e ~ start ~ length => SQLSubstring(e, start.toInt, length.toInt)
  }

  def fn: Parser[SQLExpr] = sum | avg | min | max | count | interval | exists | extract | sqlCase | substring

  def parens: Parser[SQLExpr] =  "(" ~> expr <~ ")" ^^ {
    case s:SQLSelect => s
    case e => SQLParens(e)
  }

  def factor: Parser[SQLExpr] = literal | fn | parens | select

  def term: Parser[SQLExpr] = factor ~ ((("*" | "/") ~ factor) *) ^^ {
    case first ~ rest =>
      if (rest.nonEmpty) {
        rest.foldLeft(first)({ case (lhs, (op ~ rhs)) => op match {
          case "*" => SQLMul(lhs, rhs)
          case "/" => SQLDiv(lhs, rhs)
        }
        })
      }
      else first
  }

  def expr: Parser[SQLExpr] = term ~ ((("+" | "-") ~ term) *) ^^ {
    case first ~ rest =>
      if (rest.nonEmpty) {
        rest.foldLeft(first)({ case (lhs, (op ~ rhs)) => {
          lhs match {
            // Resolve date arithmetic on the way up to make things easier later on
            case SQLDate(dateValue) => {
              rhs match {
                case SQLInterval(intervalValue, units) => {
                  val cal = java.util.Calendar.getInstance()
                  cal.setTime(dateValue)

                  val calUnits = units match {
                    case dayKw => java.util.Calendar.DAY_OF_YEAR
                    case monthKw => java.util.Calendar.MONTH
                    case yearKw => java.util.Calendar.YEAR
                  }

                  op match {
                    case "+" => cal.add(calUnits, intervalValue)
                    case "-" => cal.add(calUnits, -intervalValue)
                  }

                  SQLDate(cal.getTime)
                }
              }
            }

            // If there wasn't a date, put it into a plus / minus.
            case _ => {
              op match {
                case "+" => SQLPlus(lhs, rhs)
                case "-" => SQLMinus(lhs, rhs)
              }
            }
          }
        }
        })
      }
      else first
  }

  def between: Parser[SQLBetween] = (expr <~ betweenKw) ~ (expr <~ andKw) ~ expr ^^ {
    case lhs ~ lower ~ upper =>
      SQLBetween(lhs, lower, upper)
  }

  def in: Parser[SQLExpr] = expr ~ ((notKw ?) <~ inKw ~> "(") ~ exprs <~ ")" ^^ {
    case lhs ~ None ~ list => SQLIn(lhs, list)
    case lhs ~ Some(n) ~ list => SQLNot(SQLIn(lhs, list))
  }

  def booleanBase: Parser[SQLExpr] = expr ~ ((("<=" | ">=" | "<>" | ">" | "<" | "=" | likeKw | (notKw ~ likeKw)) ~ expr) ?) ^^ {
    case t ~ None => t
    case lhs ~ Some(op_rhs) =>
      op_rhs match {
        case op ~ rhs => op match {
          case ">" => SQLGT(lhs, rhs)
          case "<" => SQLLT(lhs, rhs)
          case "<=" => SQLLTE(lhs, rhs)
          case ">=" => SQLGTE(lhs, rhs)
          case "<>" => SQLNotEq(lhs, rhs)
          case "=" => SQLEq(lhs, rhs)
          case notKw ~ likeKw => SQLNot(SQLLike(lhs, rhs))
          case likeKw => SQLLike(lhs, rhs)
        }
      }
  }

  def booleanFactor: Parser[SQLExpr] = between | in | booleanBase | "(" ~> booleanExpr <~ ")"

  def booleanTerm: Parser[SQLExpr] = booleanFactor ~ ((andKw ~> booleanFactor) *) ^^ {
    case first ~ rest =>
//      SQLAnd(first :: rest)
      if (rest.nonEmpty)
        rest.foldLeft(first)({ case (lhs, rhs) => SQLAnd(lhs, rhs) })
      else first
  }

  def booleanExpr: Parser[SQLExpr] = booleanTerm ~ ((orKw ~> booleanTerm) *) ^^ {
    case first ~ rest =>
      if (rest.nonEmpty)
        SQLOr(first :: rest)
      else
        first
//      if (rest.nonEmpty)
//        rest.foldLeft(first)({ case (lhs, rhs) => SQLOr(lhs, rhs) })
//      else first
  }

  def exprs: Parser[List[SQLExpr]] = repsep(expr, ",")

  def resultColumn: Parser[SQLResultColumn] = expr ~ ((asKw ~> identifierName) ?) ^^ {
    case e ~ a => SQLResultColumn(e, a)
  }

  def resultColumns: Parser[List[SQLResultColumn]] = (distinctKw?) ~ resultColumn ~ (("," ~> repsep(resultColumn, ","))?) ^^ {
    case distinct ~ first ~ rest => {
      SQLResultColumn(first.expr, first.alias, distinct.isDefined) :: rest.getOrElse(List())
    }
  }

  def orderingTerm: Parser[SQLOrderingTerm] = expr ~ (descKw ?) ^^ {
    case e ~ d => SQLOrderingTerm(e, d.isDefined)
  }

  def orderingTerms: Parser[List[SQLOrderingTerm]] = repsep(orderingTerm, ",")

  def groupBy: Parser[SQLGroupBy] = groupByKw ~> exprs ~ ((havingKw ~> booleanExpr) ?) ^^ {
    case exs ~ h => SQLGroupBy(exs, h)
  }

  def select: Parser[SQLExpr] =
    (selectKw ~> resultColumns) ~
      (fromKw ~> relations) ~
      (joins ?) ~
      ((whereKw ~> booleanExpr) ?) ~
      (groupBy ?) ~
      ((orderByKw ~> orderingTerms) ?) ~
      ((asKw ~> identifierName) ?) <~ (";" ?) ^^ {
      case res ~ rels ~ j ~ w ~ g ~ o ~ a =>
        SQLSelect(res, rels, j, w, g, o, a)
    }

  def createView: Parser[SQLSelect] = (createKw ~> (tableKw | viewKw)  ~> identifierName <~ asKw <~ "(") ~ (select <~ ")" <~ (";"?)) ^^ {
    case (name ~ select) => select match {
      case s:SQLSelect => {
        s.alias = Some(name)
        s
      }
    }
  }

  def recursion: Parser[SQLRecursion] = (withKw ~> recursiveKw ~> ((forKw ~> number <~ iterationsKw)?)) ~
    (identifierName <~ asKw <~ "(") ~ (select <~ unionKw) ~ (select <~ ")") ^^ {
    case iterations ~ name ~ baseCase ~ recursiveCase => {
      val baseCaseCast = baseCase.asInstanceOf[SQLSelect]
      val recursiveCaseCast = recursiveCase.asInstanceOf[SQLSelect]
      baseCaseCast.alias = Some(name)
      recursiveCaseCast.alias = Some(name)
      iterations match {
        // TODO: We don't have subqueries anyway.
        case Some(it) => SQLRecursion(baseCaseCast, recursiveCaseCast, ITERATIONS(), it.value)
        case None => SQLRecursion(baseCaseCast, recursiveCaseCast, EPSILON(), "0")
      }
    }
  }

  def statement: Parser[SQLExpr] = select | createView | recursion

  def statements: Parser[List[SQLExpr]] = rep(statement)

  def run(query: String, relationsMap: scala.collection.mutable.Map[String, Relation]) = {
    this.parseAll(statements, query) match {
      case SQLParser.Success(sqlStatements, _) =>
        IR(sqlToIr(sqlStatements, relationsMap))
    }
  }

  def sqlToIr(statements: List[SQLExpr],
              relationsMap: scala.collection.mutable.Map[String, Relation],
              recursion: Option[Recursion] = None
             ):List[Rule] = {
    statements.flatMap(statement => {
      statement match {
        case SQLRecursion(baseCase, recursiveCase, criteria, value) => {
          val baseIr = sqlToIr(List(baseCase), relationsMap).head
          val newAttrs = baseCase.resultColumns.collect({
            case SQLResultColumn(SQLColumnName(columnName, _), ali, _) => {
              ali.getOrElse(columnName)
            }
          })

          relationsMap += baseIr.result.rel.name -> Relation(baseIr.result.rel.name,
            Schema(externalAttributeTypes = List(), externalAnnotationTypes = List(),
              attributeNames = newAttrs,
              annotationNames = baseIr.result.rel.getAnnotations().toList),
            "", false)
          val recursiveIr = sqlToIr(List(recursiveCase), relationsMap, Some(Recursion(criteria, EQUALS(), value))).head
          List(baseIr, recursiveIr)
        }
        case SQLSelect(resultColumns, relations, joins, where, groupBy, orderBy, alias) =>
          // Find all the joins specified in the where clause.
          val columnEqualities = sqlToColumnEqualities(where, joins, relationsMap.toMap)

          val irJoin = Join(sqlToIrRels(relations, joins, relationsMap.toMap, columnEqualities))
          val queryRelationNames = irJoin.rels.map(_.getName())
          val queryRelationsMap = relationsMap.filter({
            case (name, _) => queryRelationNames.contains(name)
          }).toMap

          val filter = where match {
            case Some(e) => Filters(sqlToSelections(e, relationsMap.toMap, columnEqualities))
            case None => Filters(List())
          }
          val (result, aggregations) = sqlToIrResultAndAggregations(
            resultColumns, alias, queryRelationsMap, irJoin, columnEqualities, filter
          )
          val order = Order(Attributes(result.rel.getAttributes().toList))

          val irOrderBy = orderBy match {
            case Some(o) => sqlToOrderBy(o)
            case None => OrderBy(List())
          }

          val project = Project(Attributes(
            (irJoin.rels.flatMap(rel => rel.getAttributes()).toSet --
              result.rel.getAttributes().toSet --
              aggregations.values.flatMap(_.attrs.values)).toList
          ))

          val newAttrs = resultColumns.collect({
            case SQLResultColumn(c:SQLColumnName, ali, _) => {
              ali.getOrElse(c.generate())
            }
          })

          relationsMap += result.rel.name -> Relation(result.rel.name,
            Schema(externalAttributeTypes = List(), externalAnnotationTypes = List(),
              attributeNames = newAttrs,
              annotationNames = result.rel.getAnnotations().toList),
            "", false)

          List(Rule(result, recursion, Operation("*"), order, project, irJoin, aggregations, filter, Some(irOrderBy)))
      }
    })
  }

  def extractSubqueries(where: SQLExpr):List[SQLSelect] = {
    where match {
      case SQLAnd(lhs, rhs) => extractSubqueries(lhs) ::: extractSubqueries(rhs)
      case bc:SQLBinaryComparison => extractSubqueries(bc.getLhs) ::: extractSubqueries(bc.getRhs)
      case sel:SQLSelect => {
        sel.where match {
          case Some(w) => extractSubqueries(w) :+ sel
          case None => List(sel)
        }
      }
      case _ => List()
    }
  }

  def sqlToIrResultAndAggregations(
                                    resultColumns: List[SQLResultColumn],
                                    alias: Option[String],
                                    queryRelationsMap: Map[String, Relation],
                                    irJoin: Join,
                                    columnEqualities: List[Set[SQLColumnName]],
                                    filters: Filters
                                  ):(Result, Aggregations) = {
    val name = alias match {case Some(s) => s; case None => "Result"}

    // Just handle SELECT * as a separate case
    resultColumns match {
      case List(SQLResultColumn(SQLColumnName("*", None), None, false)) => {
        val allAttrs = queryRelationsMap.values.flatMap(_.schema.attributeNames).toList
        val allAnnos = queryRelationsMap.values.flatMap(_.schema.annotationNames).toList

        val result = Result(Rel(name, Attributes(allAttrs), Annotations(allAnnos)), isIntermediate = false)
        return (result, Aggregations(List()))
      }
      case _ =>
    }

    val columnToJoinedName = columnEqualitiesToJoinedNames(columnEqualities)

    val (nameColumns, aggColumns) = resultColumns.partition(rc => rc.expr.isInstanceOf[SQLColumnName])

    val involvedAttributes = irJoin.rels.flatMap(_.attrs.values).toSet -- filters.values.map(_.attr)

    // Add all the columns to the attrs. list of the result, translating to the joined name if necessary.
    var allOutputAttributes = Set[String]()
    var outputAttributes = List[String]()
    var outputAnnotations = List[String]()
    nameColumns.foreach({
      case SQLResultColumn(c:SQLColumnName, _, _) => {
        if (queryRelationsMap.values.exists(_.schema.annotationNames.contains(c.name))) {
          outputAnnotations = c.name :: outputAnnotations
        } else {
          val outputAttribute = columnToJoinedName.getOrElse(c, c.generate())
          allOutputAttributes = allOutputAttributes ++ Set(outputAttribute)
          outputAttributes = outputAttributes ::: List(outputAttribute)
        }
      }
    })

    // Create all the aggregations.
    val aggregations = aggColumns.map(rc => {

      // Get the expression for the annotation (e.g. "AGG + 1"), and the aggregation involved. Only one aggregation
      // per annotation is allowed right now (i.e. not SUM(...) / SUM(...)).
      val (expression, agg) = getAggAndExpression(rc.expr)
      val aggOp = agg match {
        case Some(s: SQLSum) => SUM()
        case Some(a: SQLAvg) => AVG()
        case Some(c: SQLCount) => if (c.distinct) COUNT_DISTINCT() else SUM()
        case Some(m: SQLMin) => MIN()
        case Some(m: SQLMax) => MAX()
        case None => CONST()
      }

      val innerExpression = agg match {
        // For COUNT(*) we just have a blank inner expression.
        case Some(SQLCount(_, false)) => {
          LiteralExpr("", List())
        }

        case Some(a: SQLAggregation) => a.getExpr match {
          // The expression inside an aggregation can be a CASE or a normal expression.
          case SQLCase(condition, ifCase, elseCase) => {
            val involvedColumns = (
              extractColumnNames(condition) ::: extractColumnNames(ifCase) ::: extractColumnNames(elseCase)
              ).map(_.name)
            val involvedRelations = queryRelationsMap.values.filter({case relation =>
              val schema = relation.schema
              schema.annotationNames.union(schema.attributeNames).intersect(involvedColumns).nonEmpty
            }).toList
            CaseExpr(
              sqlToSelections(condition, queryRelationsMap, columnEqualities), ifCase.generate(), elseCase.generate(), involvedRelations.map(_.name)
            )
          }

          case e => {
            val involvedColumns = extractColumnNames(e).map(_.name)
            val involvedRelations = queryRelationsMap.values.filter(
              _.schema.annotationNames.intersect(involvedColumns).nonEmpty
            ).toList
            val relationToColumns = queryRelationsMap.values.flatMap(rel => {
              val columnsInRelation = rel.schema.annotationNames.intersect(involvedColumns)
              if (columnsInRelation.nonEmpty)
                Some(rel.name -> columnsInRelation)
              else
                None
            }).toMap

            val expression = e.generate()
            // TODO: Fix here too.
            val key = SQLColumnName(expression, None)
            LiteralExpr(columnToJoinedName.getOrElse(key, key.generate()), involvedRelations.map(_.name), relationToColumns)
          }
        }

        // If it isn't actually an aggregation (just an expression involving annotations), we leave the inner expression
        // blank, but it is still treated as an aggregation.
        case None => {
          val involvedColumns = extractColumnNames(rc.expr).map(_.name)
          val involvedRelations = queryRelationsMap.values.filter(
            _.schema.annotationNames.intersect(involvedColumns).nonEmpty
          ).toList
          LiteralExpr("", involvedRelations.map(_.name))
        }
      }

      val aggregatedAttributes = involvedAttributes -- allOutputAttributes

      val annoName = rc.alias.getOrElse(IRNameGenerator.nextAnno)

      Aggregation(
        annoName, "long", aggOp, Attributes(aggregatedAttributes.toList),
        "1", expression, List(), innerExpression
      )
    })

    // Once we have the aggregations built, we can take the annotation names for the result from there.
    val annotationNames = aggregations.map(_.annotation) ::: outputAnnotations
    val result = Result(Rel(name, Attributes(outputAttributes), Annotations(annotationNames)), isIntermediate = false)

    (result, Aggregations(aggregations))
  }

  def sqlToSelections(where: SQLExpr,
                      relationsMap: Map[String, Relation],
                      columnEqualities: List[Set[SQLColumnName]]
                     ):List[Selection] = {

    val columnToJoinedName = columnEqualitiesToJoinedNames(columnEqualities)

    where match {
      case SQLEq(col1: SQLColumnName, col2: SQLColumnName) => {
        val allAnnos = relationsMap.values.flatMap(_.schema.annotationNames).toSet
        if (allAnnos.contains(col2.name)) {
          val lhsName = columnToJoinedName.getOrElse(col1, col1.generate())
          List(Selection(lhsName, EQUALS(), SelectionLiteral(col2.generate())))
        } else {
          List()
        }
      }
      case bc:SQLBinaryComparison => List(bc.getSelection(columnToJoinedName))
      case SQLIn(lhs, exprs) => List(Selection(lhs.generate(), IN(), SelectionInList(exprs.map(_.generate()))))
      case SQLBetween(lhs, lower, upper) => List(
        SQLLTE(lhs, upper).getSelection(columnToJoinedName),
        SQLGTE(lhs, lower).getSelection(columnToJoinedName)
      )
      case SQLAnd(lhs, rhs) => {
        sqlToSelections(lhs, relationsMap, columnEqualities) ::: sqlToSelections(rhs, relationsMap, columnEqualities)
      }
      case SQLNot(e) => {
        val innerSelections = sqlToSelections(e, relationsMap, columnEqualities)
        innerSelections.length match {
          case 1 => {
            val innerSelection = innerSelections.head
            List(Selection(innerSelection.attr, innerSelection.operation, innerSelection.value, negated = true))
          }
          case _ => {
            throw new Exception("Negations over multiple selections not supported.")
          }
        }
      }
      case SQLOr(exprs) => {
        // OR hacked into selection here. Again, might want to do this right eventually.
        List(Selection("", OR(), SelectionOrList(exprs.map(expr => Filters(sqlToSelections(expr, relationsMap, columnEqualities))))))
      }
    }
  }

  def columnEqualitiesToJoinedNames(columnEqualities: List[Set[SQLColumnName]]) = {
    columnEqualities.flatMap(set => {
      val joinedName = set.map(_.generate()).mkString("_")
      set.map(_ -> joinedName)
    }).toMap
  }

  def sqlToColumnEqualities(where: Option[SQLExpr], joins:Option[List[SQLJoin]], relationsMap:Map[String, Relation]) = {
    val pairwiseEqualities = (where match {
      case Some(e) => sqlToPairwiseColumnEqualities(e, relationsMap)
      case None => List[Set[SQLColumnName]]()
    }) ::: (joins match {
      case Some(js) => js.flatMap(join => sqlToPairwiseColumnEqualities(join.condition, relationsMap))
    })
    mergeSets(pairwiseEqualities).filter(_.nonEmpty)
  }

  def sqlToPairwiseColumnEqualities(where: SQLExpr, relationsMap:Map[String, Relation]):List[Set[SQLColumnName]] = {
    where match {
      case SQLEq(col1: SQLColumnName, col2: SQLColumnName) => {
        val allAttrs = relationsMap.values.flatMap(_.schema.attributeNames).toSet

        if (allAttrs.contains(col1.name) && allAttrs.contains(col2.name))
          List(Set(col1, col2))
        else
          List(Set())
      }
      case SQLAnd(lhs, rhs) => sqlToPairwiseColumnEqualities(lhs, relationsMap) ::: sqlToPairwiseColumnEqualities(rhs, relationsMap)
      case _ => List(Set())
    }
  }

  def sqlToOrderBy(terms: List[SQLOrderingTerm]) = {
    OrderBy(terms.map(term => OrderingTerm(term.expr.generate(), term.desc)))
  }

  def sqlToIrRels(relations: List[SQLExpr],
                  joins: Option[List[SQLJoin]],
                  relationsMap: Map[String, Relation],
                  columnEqualities: List[Set[SQLColumnName]]) = {
    val columnToJoinedName = columnEqualitiesToJoinedNames(columnEqualities)

    val allRelations = relations.map({case r:SQLRelation => r}) ::: (joins match {
      case Some(js) => js.map(_.relation)
      case None => List[SQLRelation]()
    })

    allRelations.map({case SQLRelation(tableName, alias) => {
      val attrs = relationsMap.get(tableName) match {
        case Some(r) => r.schema.attributeNames.map(
          attrName => {
            val key = SQLColumnName(attrName, alias)
            columnToJoinedName.getOrElse(key, key.generate())
          }
        )
        case None => throw new Exception(s"""Relation "$tableName" does not exist""")
      }

      val annos = relationsMap(tableName).schema.annotationNames

//      val name = alias match {
//        case Some(a) => s"${tableName}_${a}"
//        case None => tableName
//      }

      Rel(tableName, Attributes(attrs), Annotations(annos))
    }})
  }

  def getAggAndExpression(expr: SQLExpr):(String, Option[SQLAggregation]) = {
    expr match {
      case agg:SQLAggregation => ("AGG", Some(agg))
      case binOp:SQLBinaryOperation => {
        val (lhsExpr, lhsAgg) = getAggAndExpression(binOp.getLhs())
        val (rhsExpr, rhsAgg) = getAggAndExpression(binOp.getRhs())

        val resultAgg = (lhsAgg, rhsAgg) match {
          case (Some(lhs), None) => Some(lhs)
          case (None, Some(rhs)) => Some(rhs)
          case (None, None) => None
          case (Some(lhs), Some(rhs)) => throw new Exception("Multiple aggregations in a column not currently supported.")
        }

        (lhsExpr + binOp.op + rhsExpr, resultAgg)
      }
      // If we get an extract, put it into a string. Seems less likely to break things for now, can change if
      // this doesn't work with codegen.
      case SQLExtract(col, unit) => (s"EXTRACT(${col.generate()}, $unit)", None)
      case e => (e.generate(), None)
    }
  }

  def extractColumnNames(expr:SQLExpr):List[SQLColumnName] = {
    expr match {
      case cn:SQLColumnName => List(cn)
      case bo:SQLBinaryOperation => extractColumnNames(bo.getLhs()) ::: extractColumnNames(bo.getRhs())
      case SQLParens(e) => extractColumnNames(e)
//      case SQLOr(lhs, rhs) => extractColumnNames(lhs) ::: extractColumnNames(rhs)
      case SQLOr(exprs) => exprs.flatMap(extractColumnNames(_))
      case SQLAnd(lhs, rhs) => extractColumnNames(lhs) ::: extractColumnNames(rhs)
      case bc:SQLBinaryComparison => extractColumnNames(bc.getLhs()) ::: extractColumnNames(bc.getRhs())
      case _ => List()
    }
  }

  def mergeSets[T](sets:List[Set[T]]) = {
    sets.foldLeft(List[Set[T]]())((cumulative, current) => {
      val (common, rest) = cumulative.partition(set => set.intersect(current).nonEmpty)
      (common.flatten.toSet ++ current) :: rest
    })
  }

  object IRNameGenerator {
    var colIdx = 0
    var annoIdx = 0
    val resultIdx = 0

    def nextCol = {
      val result = ('a' + colIdx).toChar.toString
      colIdx += 1
      result
    }

    def nextAnno = {
      val result = "anno" + colIdx.toString
      colIdx += 1
      result
    }

    def nextResult = {
      val result = "result" + colIdx.toString
      colIdx += 1
      result
    }
  }
}