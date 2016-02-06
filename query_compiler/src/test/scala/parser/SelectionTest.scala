package DunceCap

import org.scalatest.FunSuite

class SelectionTest extends FunSuite {
  test("Make sure we can propagate selections through the query") {
    val query = "Foo(;m:long) :- R(a,b=1),S(b,c);m=[<<COUNT(*)>>]."
    val rewritten = DCParser.parseAll(DCParser.statement, query) match {
      case DCParser.Success(parsedStatement, _) => {
        parsedStatement.propagateSelections
        parsedStatement
      }
    }
    val relSAttrB = rewritten.join.find(rel => rel.name == "S").get.attrs.find(attr => attr._1 == "b").get
    assertResult("=")(relSAttrB._2)
    assertResult("1")(relSAttrB._3)
  }

  test("MultipleSelectionsUnsupportedException thrown if b=1 and b=2") {
    val query = "Foo(;m:long) :- R(a,b=1),S(b=2,c);m=[<<COUNT(*)>>]."
    intercept[MultipleSelectionsUnsupportedException] {
      DCParser.parseAll(DCParser.statement, query) match {
        case DCParser.Success(parsedStatement, _) => {
          parsedStatement.propagateSelections
          parsedStatement
        }
      }
    }
  }

  test("MaterializationOfSelectedAttrUnsupportedException thrown if you try to materialize a selected attr") {
    val query = "Foo(a,b) :- R(a,b=1)."
    intercept[MaterializationOfSelectedAttrUnsupportedException] {
      DCParser.parseAll(DCParser.statement, query) match {
        case DCParser.Success(parsedStatement, _) => {
          parsedStatement.propagateSelections
          parsedStatement
        }
      }
    }
  }
}

