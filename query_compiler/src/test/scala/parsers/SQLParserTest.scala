package duncecap

import org.scalatest.FunSuite

class SQLParserTest extends FunSuite {


  val qc = QueryCompiler.fromDisk("/Users/andrewlamb/EmptyHeaded/docs/notebooks/graph/db/schema.bin")
  val db = qc.db
  val hash = "test"

  test("Should parse a triangle query") {

    val query =
      """
CREATE TABLE Triangle AS (
SELECT e1.a, e2.a, e3.a FROM Edge e1 JOIN Edge e2 ON e1.b = e2.a JOIN Edge e3 ON e2.b = e3.a AND e3.b = e1.a
)
      """

    val observed = SQLParser.run(query, db)
  }

  test("Should parse a projection") {
    val query =
      """CREATE TABLE ProjectedTriangle AS (
SELECT e1.a, e2.a FROM Edge e1 JOIN Edge e2 ON e1.b = e2.a JOIN Edge e3 ON e2.b = e3.a AND e3.b = e1.a
)
      """

    val observed = SQLParser.run(query, db)
  }

  test("Should parse a selection") {
    val query =
      """CREATE TABLE FliqueSel AS (
SELECT e1.a, e2.a, e3.a, e4.a FROM Edge e1 JOIN Edge e2 ON e1.b = e2.a JOIN Edge e3 ON e2.b = e3.a
JOIN Edge e4 ON e3.b = e4.a AND e4.b = e1.a JOIN Edge e5 ON e5.a = e1.a AND e5.b = e2.b
JOIN Edge e6 ON e6.a = e1.b AND e6.b = e3.b JOIN Edge e7 ON e7.a = e1.a WHERE e7.b = 0
)
      """
    SQLParser.run(query, db)
  }

  test("Should parse a count") {
    val query =
      """CREATE TABLE BarbellAgg AS (
SELECT COUNT(*) FROM Edge e1 JOIN Edge e2 ON e1.b = e2.a JOIN Edge e3 ON e2.b = e3.a AND e3.b = e1.a
JOIN Edge e4 ON e4.a = e1.b JOIN Edge e5 ON e5.a = e4.b JOIN Edge e6 ON e5.b = e6.a
JOIN Edge e7 ON e6.b = e7.a AND e7.b = e5.a
)
      """
    SQLParser.run(query, db)
  }

  test("Should parse a query with no joins") {
    val query = """CREATE TABLE N AS (SELECT SUM(e.a) FROM Edge e)"""
    SQLParser.run(query, db)
  }

  test("Should parse a query with math") {
    val query =
      """
      CREATE TABLE PageRank AS (
        SELECT e.a, 1 / N FROM Edge e
      )
      """
    SQLParser.run(query, db)
  }

  test("Should parse a query with multiple statements") {
    val query =  """
    CREATE TABLE N AS (
      SELECT SUM(e.a) FROM Edge e
    );
"""
    SQLParser.run(query, db)
  }

  test("Should join multiple tables") {
    val query = """
                  CREATE TABLE PageRank AS (
                        SELECT e.a, 1 / N FROM Edge e
                      );
CREATE TABLE PageRank AS (
SELECT e.a, 0.15+0.85*SUM(e.b) FROM Edge e JOIN PageRank pr ON e.b = pr.a JOIN InvDegree i ON pr.a = i.a
)
                """
    SQLParser.run(query, db)
  }

  test("Should parse PageRank") {
    val query = """
CREATE TABLE N AS (
    SELECT SUM(e.a) FROM Edge e
);

WITH RECURSIVE FOR 5 ITERATIONS (
CREATE TABLE PageRank AS (
    SELECT e.a, 1.0 / N FROM Edge e
)
UNION
CREATE TABLE PageRank AS (
    SELECT e.a, 0.15+0.85*SUM(e.b) FROM Edge e JOIN PageRank pr ON e.b = pr.a JOIN InvDegree i ON pr.a = i.a
)
)
    """

    SQLParser.run(query, db)
  }

  test("Should parse SSSP") {
    val query = """
WITH RECURSIVE (
CREATE TABLE SSSP AS (
    SELECT e.b, 1 FROM Edge e WHERE e.a = 107
);
UNION
CREATE TABLE SSSP AS (
    SELECT e.b, 1 + MIN(e.a) FROM Edge e
    JOIN SSSP s ON s.a = e.a
)
)
                """

    SQLParser.run(query, db)
  }
}
