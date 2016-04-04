package duncecap

import scala.util.parsing.combinator.RegexParsers

case class SQLRelation(name:String, alias:String)

abstract class SQLResult

abstract class SQLExpression extends SQLResult
case class SQLCompoundExpression(lhs:SQLExpression, op:String, rhs:SQLExpression) extends SQLExpression

abstract class SQLTerm extends SQLExpression
case class SQLLiteral(value:String) extends SQLTerm

abstract class SQLAggregation extends SQLTerm
case class SQLCount() extends SQLAggregation
case class SQLSum(column: SQLColumn) extends SQLAggregation
case class SQLMin(column: SQLColumn) extends SQLAggregation

case class SQLColumn(name:String, relation:String) extends SQLResult

abstract class SQLBooleanExpression
abstract class SQLEqualityExpression extends SQLBooleanExpression
case class SQLColumnEqualityExpression(column1: SQLColumn, column2: SQLColumn) extends SQLEqualityExpression
case class SQLLiteralEqualityExpression(column: SQLColumn, literal:SQLLiteral) extends SQLEqualityExpression
case class SQLAndExpression(expr1: SQLBooleanExpression, expr2: SQLBooleanExpression) extends SQLBooleanExpression


case class SQLJoin(rel: SQLRelation, expr:SQLBooleanExpression)

abstract class Convergence
case class IterationsConvergence(iterations: String) extends Convergence
case class EpsilonConvergence(epsilon: String) extends Convergence

case class SQLSelectStatement(
                               resultList: List[SQLResult],
                               relation: SQLRelation,
                               joins: List[SQLJoin],
                               where: Option[SQLBooleanExpression]
                             )

abstract class TopLevelSQLStatement
case class SQLCreateTableStatement(
                         resultName: String,
                         selectStatement: SQLSelectStatement
                       ) extends TopLevelSQLStatement

case class SQLRecursion(convergence:Convergence, resultName:String, baseCase: SQLSelectStatement, recursiveCase: SQLSelectStatement) extends TopLevelSQLStatement

object SQLParser extends RegexParsers {
  def run(line:String, dBInstance: DBInstance) : IR = {
    val irbuilder = new IRBuilder()
    this.parseAll(this.rule, line) match {
      case SQLParser.Success(sqlStatements, _) => {

        var relationsToSize = dBInstance.relations.map(
          relation => relation.getName() -> relation.getSchema().getAttributeTypes.length
        ).toMap

        sqlStatements.foreach({
          case SQLCreateTableStatement(resultName, SQLSelectStatement(resultList, relation, joins, where)) => {
            val irRule = sqlToIR(
              resultName, resultList,
              relation, joins, where, relationsToSize
            )
            irbuilder.addRule(irRule)

            relationsToSize += irRule.result.rel.getName() -> irRule.result.rel.getAnnotations().length
          }
          case SQLRecursion(iterations, resultName, baseCase, recursiveCase) => {

            val baseIrRule = sqlToIR(resultName, baseCase.resultList,
              baseCase.relation, baseCase.joins,
              baseCase.where, relationsToSize)

            irbuilder.addRule(baseIrRule)
            relationsToSize += baseIrRule.result.rel.getName() -> baseIrRule.result.rel.getAnnotations().length


            val recursiveIrRule = sqlToIR(resultName, recursiveCase.resultList,
              recursiveCase.relation, recursiveCase.joins,
              recursiveCase.where, relationsToSize, Some(iterations))

            irbuilder.addRule(recursiveIrRule)
            relationsToSize += recursiveIrRule.result.rel.getName() -> recursiveIrRule.result.rel.getAnnotations().length
          }
        })
      }
    }
    irbuilder.build()
  }

  def identifierName:Parser[String] = """[a-zA-Z][a-zA-Z0-9]*""".r

  def quotedString:Parser[String] = """'[^']*'""".r

  def number:Parser[String] = """\d+(.\d*)?""".r

  def op:Parser[String] = """[*/+-]""".r

  def literal:Parser[SQLLiteral] = (identifierName | number | quotedString) ^^ {case s => SQLLiteral(s)}

  def column:Parser[SQLColumn] = (identifierName <~ ".") ~ identifierName ^^ {
    case relation ~ column => {
      SQLColumn(column, relation)
    }
  }

  def sum:Parser[SQLSum] = "SUM(" ~> column <~ ")" ^^ {case column => SQLSum(column)}

  def min:Parser[SQLMin] = "MIN(" ~> column <~ ")" ^^ {case column => SQLMin(column)}

  def count:Parser[SQLCount] = "COUNT(*)" ^^ {case _ => SQLCount()}

  def aggregation:Parser[SQLAggregation] = sum | min | count

  def columns:Parser[List[SQLColumn]] = column ~ (("," ~> column)*) ^^ {
    case firstColumn ~ rest => {
      firstColumn :: rest
    }
  }

  def term:Parser[SQLTerm] = aggregation | literal

  def expression:Parser[SQLExpression] = term ~ ((op ~ expression)?) ^^ {
    case t ~ None => {
      t
    }
    case lhs ~ Some(op_rhs) => {
      op_rhs match {case o ~ rhs => SQLCompoundExpression(lhs, o, rhs)}
    }
  }

  def result:Parser[SQLResult] = column | expression

  def resultList:Parser[List[SQLResult]] = result ~ (("," ~> result)*) ^^ {
    case firstResult ~ rest => {
      firstResult :: rest
    }
  }

  def relation:Parser[SQLRelation] = identifierName ~ identifierName ^^ {
    case name ~ alias => {
      SQLRelation(name, alias)
    }
  }

<<<<<<< HEAD
  def andExpression:Parser[SQLAndExpression] = equalityExpression ~ ("AND" ~> (andExpression | equalityExpression)) ^^ {
=======
  def andExpression:Parser[SQLAndExpression] = equalityExpression ~ ("AND" ~> equalityExpression) ^^ {
>>>>>>> andy-sql-parser
    case expr1 ~ expr2 => {
      SQLAndExpression(expr1, expr2)
    }
  }

  def columnEqualityExpression:Parser[SQLColumnEqualityExpression] = (column <~ "=") ~ column ^^ {
    case column1 ~ column2 => SQLColumnEqualityExpression(column1, column2)
  }

  def literalEqualityExpression:Parser[SQLLiteralEqualityExpression] = (column <~ "=") ~ literal ^^ {
    case column ~ literal => SQLLiteralEqualityExpression(column, literal)
  }

  def equalityExpression:Parser[SQLEqualityExpression] = columnEqualityExpression | literalEqualityExpression

  def groupBy:Parser[List[SQLColumn]] = "GROUP BY" ~> columns

  def where:Parser[SQLBooleanExpression] = "WHERE" ~> (andExpression | equalityExpression)

  def join:Parser[SQLJoin] = ("JOIN" ~> relation) ~ ("ON" ~> (andExpression | equalityExpression)) ^^ {
    case relation ~ expr => {
      SQLJoin(relation, expr)
    }
  }

  def joins:Parser[List[SQLJoin]] = join*

  def select:Parser[SQLSelectStatement] = ("SELECT" ~> resultList) ~ ("FROM" ~> relation) ~ joins ~ (where?) ^^ {
    case resultColumns ~ relation ~ joins ~ where => {
      SQLSelectStatement(resultColumns, relation, joins, where)
    }
  }

  def createTableStatement:Parser[SQLCreateTableStatement] = ("CREATE TABLE" ~> identifierName <~ "AS" <~ "(") ~ select <~ ")" <~ (";"?) ^^ {
    case resultName ~ select => SQLCreateTableStatement(resultName, select)
  }

  def iterationsCriteria[Parser[String]] = "FOR" ~> number <~ "ITERATIONS"

  def recursiveStatement:Parser[SQLRecursion] = "WITH RECURSIVE" ~> (iterationsCriteria?) ~ (identifierName <~ "AS" <~ "(") ~ select ~ ("UNION" ~> select <~ ")" <~ (";"?)) ^^ {
    case it ~ resultName ~ baseCase ~ recursiveCase => {
      val iterations = it match {
        case Some(s:String) => IterationsConvergence(s)
        case None => EpsilonConvergence("0")
      }
      SQLRecursion(iterations, resultName, baseCase, recursiveCase)
    }
  }

  def rule:Parser[List[TopLevelSQLStatement]] = (createTableStatement | recursiveStatement)+

  def sqlToIR(
               resultName: String,
               resultList: List[SQLResult],
               relation: SQLRelation,
               joins: List[SQLJoin],
               where: Option[SQLBooleanExpression],
               relationsToSize: Map[String, Int],
               iterations: Option[Convergence] = None
             ) = {
    val iRNameGenerator = new IRNameGenerator

    // A list of sets of columns that must be equal by join constraints.
    val columnEquivalenceConstraints = mergeSets(
      // Look at the join conditions, take all the "column = column" expressions and map them to
      // sets of two columns
      joins.flatMap(sqlJoin => extractEqualities(sqlJoin.expr).map({
        case SQLColumnEqualityExpression(col1, col2) => Set(col1, col2)
        case _ => throw new Exception("Bad join constraint.")
      }))
    )

    var columnToIRName = columnEquivalenceConstraints.flatMap(equivalenceSet => {
      val irName = iRNameGenerator.nextCol
      equivalenceSet.map(sqlColumn => sqlColumn -> irName)
    }).toMap

    val irJoins = Join(
      (relation :: joins.map(sqlJoin => sqlJoin.rel)).map(sqlRelation => {
        val numColumns = relationsToSize.get(sqlRelation.name) match {
          case Some(i:Int) => i
          case None => throw new Exception("Relation " + sqlRelation.name + " not found.")
        }
        val irNames = (0 until numColumns).map({case idx => {
          val key = SQLColumn(idxToColumnName(idx), sqlRelation.alias)
          val irName = columnToIRName.get(key) match {
            case Some(name:String) => name
            case None => {
              val name = iRNameGenerator.nextCol
              columnToIRName += (key -> name)
              name
            }
          }

          irName
        }}).toList

        Rel(sqlRelation.name, Attributes(irNames))
      })
    )

    val outputIrNames = resultList.collect({
      case c:SQLColumn => columnToIRName.get(c) match {
        case Some(irName:String) => irName
      }
    })
    val projectIrNames = (columnToIRName.values.toSet -- outputIrNames.toSet).toList

    val aggs = resultList.collect({
      case expr:SQLExpression => {
        val annotationExpr = extractAnnotationExpr(expr)
        extractAggregation(expr) match {
          case Some(SQLSum(column)) => {
            val columnIrName = columnToIRName.get(column) match {
              case Some(name:String) => name
              case None => throw new Exception("Bad column name in sum")
            }
            Aggregation(iRNameGenerator.nextAnno, "float", SUM(), Attributes(List(columnIrName)), "1", annotationExpr, List())
          }
          case Some(SQLMin(column)) => {
            val columnIrName = columnToIRName.get(column) match {
              case Some(name:String) => name
              case None => throw new Exception("Bad column name in min")
            }
            Aggregation(iRNameGenerator.nextAnno, "long", MIN(), Attributes(List(columnIrName)), "1", annotationExpr, List())
          }
          case Some(c:SQLCount) => {
            Aggregation(iRNameGenerator.nextAnno, "long", SUM(), Attributes(columnToIRName.values.toList.distinct), "1", annotationExpr, List())
          }
          case None => {
            Aggregation(iRNameGenerator.nextAnno, "float", CONST(), Attributes(List()), annotationExpr, "1", List())
          }
        }
      }
    })

    val outputAnnotations = Annotations(aggs.map(agg => agg.annotation))

    val result = Result(Rel(resultName, Attributes(outputIrNames), outputAnnotations), false)
    val project = Project(Attributes(projectIrNames))
    val order = Order(Attributes(outputIrNames ::: projectIrNames))

    val filter = Filters(where match {
      case Some(expr:SQLBooleanExpression) => {
        extractEqualities(expr).map({
          case SQLLiteralEqualityExpression(column, literal) => {
            val irName = columnToIRName.get(column) match {
              case Some(name:String) => name
              case None => throw new Exception("Bad column name in selection")
            }
            Selection(irName, EQUALS(), literal.value)
          }
        })
      }
      case None => List()
    })

    val recursion = iterations match {
      case Some(IterationsConvergence(i:String)) => Some(Recursion(ITERATIONS(), EQUALS(), i))
      case Some(EpsilonConvergence(e:String)) => Some(Recursion(EPSILON(), EQUALS(), e))
      case None => None
    }

    Rule(result, recursion, Operation("*"), order, project, irJoins, Aggregations(aggs), filter)
  }

  class IRNameGenerator {
    var colIdx = 0
    var annoIdx = 0

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
  }

  def extractAnnotationExpr(expr:SQLExpression):String = {
    expr match {
      case _:SQLAggregation => "AGG"
      case l:SQLLiteral => l.value
      case SQLCompoundExpression(lhs, op, rhs) => {
        List(extractAnnotationExpr(lhs), op, extractAnnotationExpr(rhs)).mkString(" ")
      }
    }
  }

  def extractAggregation(expr:SQLExpression):Option[SQLAggregation] = {
    expr match {
      case s:SQLSum => Some(s)
      case c:SQLCount => Some(c)
      case m:SQLMin => Some(m)
      case l:SQLLiteral => None
      case SQLCompoundExpression(lhs, op, rhs) => {
        val lhsRes = extractAggregation(lhs)
        if (lhsRes.isDefined) lhsRes else extractAggregation(rhs)
      }
    }
  }

  def idxToColumnName(idx:Int) = {
    ('a' + idx).toChar.toString
  }

  def mergeSets[T](sets:List[Set[T]]) = {
    sets.foldLeft(List[Set[T]]())((cumulative, current) => {
      val (common, rest) = cumulative.partition(set => set.intersect(current).nonEmpty)
      (common.flatten.toSet ++ current) :: rest
    })
  }

  def extractEqualities(expr:SQLBooleanExpression):List[SQLEqualityExpression] = {
    expr match {
      case eqExpr:SQLEqualityExpression => List(eqExpr)
      case SQLAndExpression(expr1, expr2) => extractEqualities(expr1) ::: extractEqualities(expr2)
    }
  }
}