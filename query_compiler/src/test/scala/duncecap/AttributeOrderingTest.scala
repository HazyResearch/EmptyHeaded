package DunceCap

import DunceCap.attr._
import org.scalatest.FunSuite

class AttributeOrderingTest extends FunSuite {
  test("Can reorder attributes so that equality selected ones are at the front") {
    val partitioned = GHDSolver.partition_equality_selected(
      List[Attr]("a","b","x","c", "y", "z"),
      List[(Attr, SelectionOp, SelectionVal)](
        ("a", "", ""),
        ("y", "=", "1"),
        ("b", "", ""),
        ("x", "=", "1"),
        ("c", "", ""),
        ("z", "<", "100"),
        ("z", "=", "5")
      ))
    assertResult(List[Attr]("x", "y", "z", "a", "b", "c"))(partitioned)
  }

  test("Can order attributes, taking into account GHD structure, what's materialized, and what's equality-selected") {
    // TODO
  }
}
