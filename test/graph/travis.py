import numpy as np
import pandas as pd
from emptyheaded import *

check_big_out = False

## TODO: 
## 4-Clique SQL 
## Fix Barbell and 4-Clique Selection Order

class ResultError(Exception):
    pass

def lollipop_agg(db):
  lolli_agg = \
"""
LollipopAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),z:long<-[COUNT(*)].
"""
  print "\nLollipop AGG"
  db.eval(lolli_agg)

  tri = db.get("LollipopAgg")
  df = tri.getDF()
  print df

  if tri.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

  if df.iloc[0][0] != 1426911480L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def lollipop_agg_sql(db):
  lolli_agg = \
      """
      CREATE TABLE LollipopAggSQL AS (
      SELECT COUNT(*)
      FROM Edge e1
      JOIN Edge e2 ON e1.b = e2.a
      JOIN Edge e3 ON e2.b = e3.a AND e1.a = e3.b
      JOIN Edge e4 ON e1.a = e4.a)
      """
  print "\nLollipop AGG SQL"
  db.eval(lolli_agg, useSql=True)

  tri = db.get("LollipopAggSQL")
  df = tri.getDF()
  print df

  if tri.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

  if df.iloc[0][0] != 1426911480L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def barbell_agg(db):
  b_agg = \
"""
BarbellAgg(;w) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z),w:long<-[COUNT(*)].
"""
  print "\nBarbell AGG"
  db.eval(b_agg)

  tri = db.get("BarbellAgg")
  df = tri.getDF()
  print df

  if tri.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

  if df.iloc[0][0] != 20371831447136L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def barbell_agg_sql(db):
  b_agg = \
      """
      CREATE TABLE BarbellAggSQL AS (
      SELECT COUNT(*)
      FROM Edge e1
      JOIN Edge e2 ON e1.b = e2.a
      JOIN Edge e3 ON e2.b = e3.a AND e3.b = e1.a
      JOIN Edge e4 ON e4.a = e1.b
      JOIN Edge e5 ON e5.a = e4.b
      JOIN Edge e6 ON e5.b = e6.a
      JOIN Edge e7 ON e6.b = e7.a AND e7.b = e5.a
      )
      """

  b_agg = \
  """
CREATE TABLE BarbellAggSQL AS (
  SELECT COUNT(*) FROM Edge e1 JOIN Edge e2 ON e1.b = e2.a JOIN Edge e3 ON e2.b = e3.a AND e3.b = e1.a
JOIN Edge e4 ON e4.a = e1.b JOIN Edge e5 ON e5.a = e4.b JOIN Edge e6 ON e5.b = e6.a
JOIN Edge e7 ON e6.b = e7.a AND e7.b = e5.a
)
"""
  print "\nBarbell AGG SQL"
  db.eval(b_agg, useSql=True)

  tri = db.get("BarbellAggSQL")
  df = tri.getDF()
  print df

  if tri.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

  if df.iloc[0][0] != 20371831447136L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def barbell_materialized(db):
  barbell = \
"""
Barbell(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z).
"""
  print "\nBARBELL"
  db.eval(barbell)

  if check_big_out:
    tri = db.get("Barbell")
    df = tri.getDF()

    if tri.num_rows != 56L:
      raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
    row0 = df.iloc[55]
    if row0[0] != 5l or row0[1] != 3l or row0[2] != 4l or row0[3] != 3l or row0[4] != 4l or row0[5] != 5l: #5  4  4  3  5  3
      raise ResultError("ROW0 INCORRECT: " + str(row0))

def barbell_materialized_sql(db):
  barbell = \
    """
    CREATE TABLE BarbellSQL AS (
    SELECT e1.a, e2.a, e3.b, e5.a, e6.a, e7.b
    FROM Edge e1
    JOIN Edge e2 ON e1.b = e2.a
    JOIN Edge e3 ON e2.b = e3.b 
                AND e3.a = e1.a
    JOIN Edge e4 ON e4.a = e1.a
    JOIN Edge e5 ON e5.a = e4.b
    JOIN Edge e6 ON e5.b = e6.a
    JOIN Edge e7 ON e6.b = e7.b 
                AND e7.a = e5.a
    )
    """
  print "\nBARBELL SQL"
  db.eval(barbell, useSql=True)

  if check_big_out:
    tri = db.get("BarbellSQL")
    df = tri.getDF()

    if tri.num_rows != 56L:
      raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
    row0 = df.iloc[55]
    # Rows are permuted here too.
    if row0[0] != 5l or row0[1] != 3l or row0[2] != 4l or row0[3] != 3l or row0[4] != 4l or row0[5] != 5l: #5  3  4  3  4  5
      raise ResultError("ROW0 INCORRECT: " + str(row0))

def lollipop_materialized(db):
  lollipop = \
"""
Lollipop(a,b,c,x) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x).
"""
  print "\nLOLLIPOP"
  db.eval(lollipop)

  tri = db.get("Lollipop")
  df = tri.getDF()

  if tri.num_rows != 28L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
  row0 = df.iloc[27]
  if row0[0] != 5l or row0[1] != 3l or row0[2] != 4l or row0[3] != 4l:
    raise ResultError("ROW0 INCORRECT: " + str(row0))

def lollipop_materialized_sql(db):
  lollipop = \
    """
    CREATE TABLE LollipopSQL AS (
    SELECT e1.a, e2.a, e3.a, e4.b
    FROM Edge e1
    JOIN Edge e2 ON e1.b = e2.a
    JOIN Edge e3 ON e2.b = e3.a AND e1.a = e3.b
    JOIN Edge e4 ON e1.a = e4.a)
    """
  print "\nLOLLIPOP SQL"
  db.eval(lollipop, useSql=True)

  if check_big_out:
    tri = db.get("LollipopSQL")
    df = tri.getDF()

    if tri.num_rows != 28L:
      raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
    row0 = df.iloc[27]
    if row0[0] != 5l or row0[1] != 3l or row0[2] != 4l or row0[3] != 4l:
      raise ResultError("ROW0 INCORRECT: " + str(row0))

def triangle_agg(db):
  tri_agg = \
"""
TriangleAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),z:long<-[COUNT(*)].
"""
  print "\nTRIANGLE AGG"
  db.eval(tri_agg)

  tri = db.get("TriangleAgg")
  df = tri.getDF()
  print df

  if tri.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

  if df.iloc[0][0] != 1612010L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def triangle_agg_sql(db):
  tri_agg = \
    """
    CREATE TABLE TriangleAggSQL AS (
    SELECT COUNT(*)
    FROM Edge e1
    JOIN Edge e2 ON e1.b = e2.a
    JOIN Edge e3 ON e2.b = e3.b AND e3.a = e1.a
    )
    """
  print "\nTRIANGLE AGG SQL"
  db.eval(tri_agg, useSql=True)

  tri = db.get("TriangleAggSQL")
  df = tri.getDF()
  print df

  if tri.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

  if df.iloc[0][0] != 1612010L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def four_clique_agg(db):
  flique = \
"""
FliqueAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),z:long<-[COUNT(*)].
"""
  print "\nFOUR CLIQUE AGG"
  db.eval(flique)

  tri = db.get("FliqueAgg")
  df = tri.getDF()
  print df

  if tri.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

  if df.iloc[0][0] != 30004668L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def four_clique_agg_sql(db):
  flique = \
    """
    CREATE TABLE FliqueAggSQL AS (
    SELECT COUNT(*)
    FROM Edge e1
    JOIN Edge e2 ON e1.b = e2.a
    JOIN Edge e3 ON e2.b = e3.b AND e3.a = e1.a
    JOIN Edge e4 ON e4.a = e1.a
    JOIN Edge e5 ON e5.a = e1.b AND e4.b = e5.b
    JOIN Edge e6 ON e6.a = e2.b AND e6.b = e5.b
    )
    """
  print "\nFOUR CLIQUE AGG SQL"
  db.eval(flique, useSql=True)

  tri = db.get("FliqueAggSQL")
  df = tri.getDF()
  print df

  if tri.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

  if df.iloc[0][0] != 30004668L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def triangle_project(db):
  triangle = \
"""
TriangleProj(a,b) :- Edge(a,b),Edge(b,c),Edge(a,c).
"""
  print "\nTRIANGLE PROJECT"
  ir = db.optimize(triangle)

  db.eval(triangle)

  tri = db.get("TriangleProj")
  df = tri.getDF()

  if tri.num_rows != 85658l:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
  row0 = df.iloc[0]
  print row0
  if row0[0] != 6l or row0[1] != 5l: #(6l,5l,2l)
    raise ResultError("ROW0 INCORRECT: " + str(row0))

def triangle_materialized(db):
  triangle = \
"""
Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).
"""
  print "\nTRIANGLE"
  db.eval(triangle)

  tri = db.get("Triangle")
  df = tri.getDF()

  if tri.num_rows != 1612010L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
  row0 = df.iloc[0]
  print row0
  if row0[0] != 6l or row0[1] != 5l or row0[2]!=2l: #(6l,5l,2l)
    raise ResultError("ROW0 INCORRECT: " + str(row0))

def triangle_materialized_sql(db):
  triangle = \
    """
    CREATE TABLE TriangleSQL AS (
    SELECT e1.a, e2.a, e3.b
    FROM Edge e1
    JOIN Edge e2 ON e1.b = e2.a
    JOIN Edge e3 ON e2.b = e3.b AND e3.a = e1.a
    )
    """
  print "\nTRIANGLE SQL"
  db.eval(triangle, useSql=True)

  tri = db.get("TriangleSQL")
  df = tri.getDF()

  if tri.num_rows != 1612010L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
  # The query appears to be the same as the Datalog triangle, but the columns
  # are permuted for some reason.
  if len(df[(df[0] == 2l) & (df[1] == 6l) & (df[2] == 5l)]) != 1: #(2l,6l,5l)
    raise ResultError("ROW (2, 6, 5) NOT FOUND")

def four_clique_materialized(db):
  four_clique = \
"""
Flique(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d).
"""
  print "\nFOUR CLIQUE"
  db.eval(four_clique)

  tri = db.get("Flique")
  df = tri.getDF()

  if tri.num_rows != 30004668L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
  row0 = df.iloc[0]
  if row0[0]!= 9 and row0[1] != 8 and row0[2] != 7 and row0[3] != 0:
    raise ResultError("ROW0 INCORRECT: " + str(row0))

def four_clique_materialized_sql(db):
  four_clique = \
    """
    CREATE TABLE FliqueSQL AS (
    SELECT e1.a, e2.a, e3.b, e4.b
    FROM Edge e1
    JOIN Edge e2 ON e1.b = e2.a
    JOIN Edge e3 ON e2.b = e3.b AND e3.a = e1.a
    JOIN Edge e4 ON e4.a = e1.a
    JOIN Edge e5 ON e5.a = e1.b AND e4.b = e5.b
    JOIN Edge e6 ON e6.a = e2.b AND e6.b = e5.b
    )
    """
  print "\nFOUR CLIQUE SQL"
  db.eval(four_clique, useSql=True)

  tri = db.get("FliqueSQL")
  df = tri.getDF()

  if tri.num_rows != 30004668L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
  row0 = df.iloc[0]
  if row0[0]!= 9 and row0[1] != 8 and row0[2] != 7 and row0[3] != 0:
    raise ResultError("ROW0 INCORRECT: " + str(row0))

def four_clique_agg_sel(db):
  fourclique_sel_agg = \
"""
FliqueSelAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=0,z:long<-[COUNT(*)].
"""
  print "\n4 CLIQUE SELECTION AGG"
  db.eval(fourclique_sel_agg)

  foursel = db.get("FliqueSelAgg")
  df = foursel.getDF()

  if foursel.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(foursel.num_rows))

  if df.iloc[0][0] != 3759972L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def four_clique_agg_sel_sql(db):
  fourclique_sel_agg = \
    """
    CREATE TABLE FliqueSelAggSQL AS (
    SELECT COUNT(*)
    FROM Edge e1
    JOIN Edge e2 ON e1.b = e2.a
    JOIN Edge e3 ON e2.b = e3.b AND e3.a = e1.a
    JOIN Edge e4 ON e4.a = e1.a
    JOIN Edge e5 ON e5.a = e1.b AND e4.b = e5.b
    JOIN Edge e6 ON e6.a = e2.b AND e6.b = e5.b
    JOIN Edge e7 ON e7.a = e1.a
    WHERE e7.b = 0
    )
    """
  print "\n4 CLIQUE SELECTION AGG SQL"
  db.eval(fourclique_sel_agg, useSql=True)

  foursel = db.get("FliqueSelAggSQL")
  df = foursel.getDF()

  if foursel.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(foursel.num_rows))

  if df.iloc[0][0] != 3759972L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def four_clique_sel(db):
  fourclique_sel = \
"""
FliqueSel(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=0.
"""
  print "\n4 CLIQUE SELECTION"
  db.eval(fourclique_sel)

  foursel = db.get("FliqueSel")
  df = foursel.getDF()

  if foursel.num_rows != 3759972L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
  row0 = df.iloc[0]
  if row0[0] != 1l or row0[1] != 0l or row0[2] != 48l or row0[3]!=53l: #(6l,5l,2l)
    raise ResultError("ROW0 INCORRECT: " + str(row0))

def four_clique_sel_sql(db):
  fourclique_sel = \
    """
    CREATE TABLE FliqueSelSQL AS (
    SELECT e1.a, e2.a, e3.a, e4.b
    FROM Edge e1
    JOIN Edge e2 ON e1.b = e2.a
    JOIN Edge e3 ON e2.b = e3.a
    JOIN Edge e4 ON e3.b = e4.b AND e4.a = e1.a
    JOIN Edge e5 ON e5.a = e1.a AND e5.b = e2.b
    JOIN Edge e6 ON e6.a = e1.b AND e6.b = e3.b
    JOIN Edge e7 ON e7.a = e1.a
    WHERE e7.b = 0
    )
    """
  print "\n4 CLIQUE SELECTION SQL"
  db.eval(fourclique_sel, useSql=True)

  foursel = db.get("FliqueSelSQL")
  df = foursel.getDF()

  if foursel.num_rows != 3759972L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
  row0 = df.iloc[0]
  if row0[0] != 1l or row0[1] != 0l or row0[2] != 48l or row0[3]!=53l:
    raise ResultError("ROW0 INCORRECT: " + str(row0))

def barbell_agg_sel(db):
  barbell_sel_agg = \
"""
BarbellSelAgg(;w) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,p),Edge(p,x),Edge(x,y),Edge(y,z),Edge(x,z),p=6,w:long<-[COUNT(*)].
"""
  print "\nBARBELL SELECTION AGG"
  db.eval(barbell_sel_agg)

  bs = db.get("BarbellSelAgg")
  df = bs.getDF()

  if bs.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(bs.num_rows))

  if df.iloc[0][0] != 26936100L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def barbell_agg_sel_sql(db):
  barbell_sel_agg = \
    """
    CREATE TABLE BarbellSelAggSQL AS (
    SELECT COUNT(*)
    FROM Edge e1
    JOIN Edge e2 ON e1.a = e2.a
    JOIN Edge e3 ON e2.b = e3.b AND e3.a = e1.b
    JOIN Edge e4 ON e4.a = e1.b
    JOIN Edge e5 ON e4.b = e5.a
    JOIN Edge e6 ON e6.a = e5.b
    JOIN Edge e7 ON e6.b = e7.b
    JOIN Edge e8 ON e7.a = e8.b AND e8.a = e6.a
    WHERE e5.a = 6
    )
    """
  print "\nBARBELL SELECTION AGG SQL"
  db.eval(barbell_sel_agg, useSql=True)

  if check_big_out:
    bs = db.get("BarbellSelAggSQL")
    df = bs.getDF()

    if bs.num_rows != 0:
      raise ResultError("NUMBER OF ROWS INCORRECT: " + str(bs.num_rows))

    if df.iloc[0][0] != 26936100L:
      raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def barbell_sel(db):
  barbell_s = \
"""
BarbellSel(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,p),Edge(p,x),Edge(x,y),Edge(y,z),Edge(x,z),p=6.
"""
  print "\nBARBELL SELECTION"
  db.eval(barbell_s)

  if check_big_out: 
    bs = db.get("BarbellSel")
    if bs.num_rows != 26936100L:
      raise ResultError("NUMBER OF ROWS INCORRECT: " + str(bs.num_rows))

def barbell_sel_sql(db):
  barbell_s = \
    """
    CREATE TABLE BarbellSelSQL AS (
    SELECT e1.a, e2.b, e3.a, e6.a, e7.b, e8.b
    FROM Edge e1
    JOIN Edge e2 ON e1.a = e2.a
    JOIN Edge e3 ON e2.b = e3.b AND e3.a = e1.b
    JOIN Edge e4 ON e4.a = e1.b
    JOIN Edge e5 ON e4.b = e5.a
    JOIN Edge e6 ON e6.a = e5.b
    JOIN Edge e7 ON e6.b = e7.b
    JOIN Edge e8 ON e7.a = e8.b AND e8.a = e6.a
    WHERE e5.a = 6
    )
    """
  print "\nBARBELL SELECTION SQL"
  db.eval(barbell_s, useSql=True)

  if check_big_out: 
    bs = db.get("BarbellSelSQL")
    if bs.num_rows != 26936100L:
      raise ResultError("NUMBER OF ROWS INCORRECT: " + str(bs.num_rows))

def sssp(db):
  paths = \
"""
SSSP(x;y) :- Edge(w,x),w=0,y:long <- [1].
SSSP(x;y)*[i=3] :- Edge(w,x),SSSP(w),y:long <- [1+MIN(w;1)].
"""
  print "\nSSSP"
  db.eval(paths)
  bs = db.get("SSSP")
  df = bs.getDF()
  if df.iloc[1000][1] != 2:
    raise ResultError("SSSP value incorrect: " + str(df.iloc[1000][1]))

def sssp_sql(db):
  paths = \
    """
WITH RECURSIVE SSSPSQL AS (
SELECT e.b, 1 FROM Edge e WHERE e.a = 0
UNION
SELECT e.b, 1 + MIN(e.a) FROM Edge e JOIN SSSPSQL s ON s.b = e.a
)
    """
  print "\nSSSP SQL"
  db.eval(paths, useSql=True)
  bs = db.get("SSSPSQL")
  df = bs.getDF()
  if df.iloc[1000][1] != 2:
    raise ResultError("SSSP value incorrect: " + str(df.iloc[1000][1]))

def pagerank(db):
  pr="""
N(;w) :- Edge(x,y),w:long<-[SUM(x;1)].
PageRank(x;y) :- Edge(x,z),y:float<-[(1.0/N)].
PageRank(x;y)*[i=5]:-Edge(x,z),PageRank(z),InvDegree(z),y:float <- [0.15+0.85*SUM(z;1.0)].
"""
  print "\nPAGERANK"
  db.eval(pr)
  bs = db.get("PageRank")
  df = bs.getDF()
  if (df.iloc[0][1]-15.227079960463206) > 0.0001:
    raise ResultError("PageRank value incorrect: " + str(df.iloc[0][1]))

def pagerank_sql(db):
  pr="""
CREATE TABLE N AS (
    SELECT SUM(e.a) FROM Edge e
);

WITH RECURSIVE FOR 5 ITERATIONS PageRankSQL AS (
SELECT e.a, 1.0 / N FROM Edge e
UNION
SELECT e.a, 0.15+0.85*SUM(e.b) FROM Edge e JOIN PageRankSQL pr ON e.b = pr.a JOIN InvDegree i ON pr.a = i.a
)
"""
  print "\nPAGERANK SQL"
  db.eval(pr, useSql=True)
  bs = db.get("PageRankSQL")
  df = bs.getDF()
  if (df.iloc[0][1]-15.227079960463206) > 0.0001:
    raise ResultError("PageRank value incorrect: " + str(df.iloc[0][1]))

def test_pruned():
  build = True
  ratings = pd.read_csv(os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/data/facebook_pruned.tsv",\
  sep='\t',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

  graph = Relation(
    name="Edge",
    dataframe=ratings,
    attribute_names=["a","b"])

  if build:
    db = Database.create(
      Config(num_threads=4),
      os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/databases/db_pruned",
      [graph])
    db.build()
  db = Database.from_existing(os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/databases/db_pruned")

  triangle_project(db)
  triangle_materialized(db)
  triangle_materialized_sql(db)
  triangle_agg(db)
  triangle_agg_sql(db)
  four_clique_materialized(db)
  #four_clique_materialized_sql(db)
  four_clique_agg(db)
  four_clique_agg_sql(db)

def test_duplicated():
  build = True
  ratings = pd.read_csv(os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/data/facebook_duplicated.tsv",\
  sep='\t',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

  graph = Relation(
    name="Edge",
    dataframe=ratings,
    attribute_names=["a","b"])

  deg = pd.read_csv(os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/data/inv_degree.tsv",\
  sep='\t',\
  names=["0","a_0"],\
  dtype={"0":np.uint32,"a_0":np.float32})

  inv_degree = Relation(
    name="InvDegree",
    dataframe=deg,
    attribute_names=["a"])

  if build:
    db = Database.create(
      Config(num_threads=4),
      os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/databases/db_duplicated",
      [graph,inv_degree])
    db.build()
  db = Database.from_existing(os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/databases/db_duplicated")

  #lollipop_agg(db)
  #lollipop_agg_sql(db)
  #barbell_agg(db)
  #barbell_agg_sql(db)
  #four_clique_agg_sel(db)
  #four_clique_agg_sel_sql(db)
  #four_clique_sel(db)
  #four_clique_sel_sql(db)
  #barbell_agg_sel(db)
  #barbell_agg_sel_sql(db)
  #barbell_sel(db)
  #barbell_sel_sql(db)
  #pagerank(db)
  #pagerank_sql(db)
  sssp(db)
  #sssp_sql(db)

def test_simple():
  build = True
  ratings = pd.read_csv(os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/data/simple.tsv",\
  sep='\t',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

  graph = Relation(
    name="Edge",
    dataframe=ratings,
    attribute_names=["a","b"])

  if build:
    db = Database.create(
      Config(num_threads=4),
      os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/databases/db_simple",
      [graph])
    db.build()
  db = Database.from_existing(os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/databases/db_simple")

  lollipop_materialized(db)
  #lollipop_materialized_sql(db)
  barbell_materialized(db)
  #barbell_materialized_sql(db)

if(len(sys.argv) < 2):
  check_big_out = True

#basically the main method down here.
start()
os.system("rm -rf "+os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/databases"+" && mkdir -p "+os.path.expandvars("$EMPTYHEADED_HOME")+"/test/graph/databases")
#test_pruned()
test_duplicated()
#test_simple()
stop()
