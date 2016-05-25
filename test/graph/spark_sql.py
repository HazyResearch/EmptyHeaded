import time
import os
import argparse

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import Row


def test_aggregation_query(query, query_name):
    if sql_context is not None:
        print("\nTESTING {0}\nSPARK SQL\n".format(query_name) + "#" * 80)

        result_set = sql_context.sql(query)
        start = time.time()
        count = result_set.collect()[0][0]
        end = time.time()

        print("ELAPSED TIME: {0} s".format(end - start))
        print("FOUND {0}.".format(count))


def test_triangle_agg(table_name):
    query = """
      SELECT COUNT(*) FROM
      {table_name} e1
      JOIN {table_name} e2 ON e1.dst = e2.src
      JOIN {table_name} e3 ON e1.src = e3.src AND e2.dst = e3.dst
      """.format(table_name=table_name)

    test_aggregation_query(query, "TRIANGLE_AGG")


def test_triangle_materialized(table_name):
    query = """
          SELECT e1.src, e1.dst, e3.dst FROM
          {table_name} e1
          JOIN {table_name} e2 ON e1.dst = e2.src
          JOIN {table_name} e3 ON e1.src = e3.src AND e2.dst = e3.dst
          """.format(table_name=table_name)

    print("\nTESTING TRIANGLE MATERIALIZED\nSPARK SQL\n" + "#" * 80)

    triangles = sql_context.sql(query)
    start = time.clock()
    result = triangles.collect()[0]
    end = time.clock()

    print("ELAPSED TIME: {0} s".format(end - start))
    print("FIRST TRIANGLE: {0}.".format(result))


def test_lollipop_agg(table_name):
    query = """
      SELECT COUNT(*)
      FROM {table_name} e1
      JOIN {table_name} e2 ON e1.dst = e2.src
      JOIN {table_name} e3 ON e2.dst = e3.src AND e1.src = e3.dst
      JOIN {table_name} e4 ON e1.src = e4.src
      """.format(table_name=table_name)

    test_aggregation_query(query, "LOLLIPOP_AGG")


def test_barbell_agg(table_name):
    query = """
      SELECT COUNT(*)
      FROM {table_name} e1
      JOIN {table_name} e2 ON e1.dst = e2.src
      JOIN {table_name} e3 ON e2.dst = e3.src AND e3.dst = e1.src
      JOIN {table_name} e4 ON e4.src = e1.dst
      JOIN {table_name} e5 ON e5.src = e4.dst
      JOIN {table_name} e6 ON e5.dst = e6.src
      JOIN {table_name} e7 ON e6.dst = e7.src AND e7.dst = e5.src
      """.format(table_name=table_name)

    test_aggregation_query(query, "BARBELL_AGG")


def test_four_clique_agg_sel(table_name):
    query = """
        SELECT COUNT(*)
        FROM {table_name} e1
        JOIN {table_name} e2 ON e1.dst = e2.src
        JOIN {table_name} e3 ON e2.dst = e3.dst AND e3.src = e1.src
        JOIN {table_name} e4 ON e4.src = e1.src
        JOIN {table_name} e5 ON e5.src = e1.dst AND e4.dst = e5.dst
        JOIN {table_name} e6 ON e6.src = e2.dst AND e6.dst = e5.dst
        JOIN {table_name} e7 ON e7.src = e1.src
        WHERE e7.dst = 0
        """.format(table_name=table_name)

    test_aggregation_query(query, "FOUR_CLIQUE_AGG_SEL")


def test_four_clique_agg(table_name):
    query = """
        SELECT COUNT(*)
        FROM {table_name} e1
        JOIN {table_name} e2 ON e1.dst = e2.src
        JOIN {table_name} e3 ON e2.dst = e3.dst AND e3.src = e1.src
        JOIN {table_name} e4 ON e4.src = e1.src
        JOIN {table_name} e5 ON e5.src = e1.dst AND e4.dst = e5.dst
        JOIN {table_name} e6 ON e6.src = e2.dst AND e6.dst = e5.dst
        """.format(table_name=table_name)

    test_aggregation_query(query, "FOUR_CLIQUE_AGG")


def test_all_queries(path):
    # Setup SparkSQL
    lines = sc.textFile(path)
    parts = lines.map(lambda l: l.split())
    edges = parts.map(lambda p: Row(src=int(p[0]), dst=int(p[1])))
    df = sql_context.createDataFrame(edges)
    
    table_name = os.path.basename(path)[:-4]
    df.registerTempTable(table_name)

    test_triangle_agg(table_name)
    test_triangle_materialized(table_name)
    test_lollipop_agg(table_name)
    test_barbell_agg(table_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Runs SparkSQL on graph datasets. Should work locally or"
                    " in a cluster."
    )
    parser.add_argument("test_dir")
    args = parser.parse_args()

    sc = SparkContext(appName="SparkSQL")
    sql_context = SQLContext(sc)

    print("\nTESTING SIMPLE")
    test_all_queries(args.test_dir + "/simple.tsv")
    print("\nTESTING DUPLICATED")
    test_all_queries(args.test_dir + "/facebook_duplicated.tsv")
    print("\nTESTING PRUNED")
    test_all_queries(args.test_dir + "/facebook_pruned.tsv")

    sc.stop()
