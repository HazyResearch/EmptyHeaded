import numpy as np
import pandas as pd
from emptyheaded import *

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

def barbell_agg(db):
  b_agg = \
"""
BarbellAgg(;w) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z),w:long<-[COUNT(*)].
"""
  print "\nLollipop AGG"
  db.eval(b_agg)

  tri = db.get("BarbellAgg")
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

  tri = db.get("Barbell")
  df = tri.getDF()

  if tri.num_rows != 56L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
  row0 = df.iloc[55]
  if row0[0] != 5l or row0[1] != 4l or row0[2] != 5l or row0[3] != 3l or row0[4] != 4l or row0[5] != 3l: #5  4  4  3  5  3
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
  if row0[0] != 5l or row0[1] != 4l or row0[2] != 4l or row0[3] != 3l:
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
  if row0[0] != 6l or row0[1] != 5l or row0[2]!=2l: #(6l,5l,2l)
    raise ResultError("ROW0 INCORRECT: " + str(row0))

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

def barbell_sel(db):
  barbell_s = \
"""
BarbellSel(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,p),Edge(p,x),Edge(x,y),Edge(y,z),Edge(x,z),p=6.
"""
  print "\nBARBELL SELECTION"
  db.eval(barbell_s)

  bs = db.get("BarbellSel")
  if bs.num_rows != 26936100L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(bs.num_rows))

def sssp(db):
  paths = \
"""
SSSP(x;y) :- Edge(w,x),w=0,y:long <- [1].
SSSP(x;y)*[c=0] :- Edge(w,x),SSSP(w),y:long <- [1+MIN(w;1)].
"""
  print "\nSSSP"
  db.eval(paths)
  bs = db.get("SSSP")
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

def test_pruned():
  build = True
  ratings = pd.read_csv(os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/facebook_pruned.tsv",\
  sep='\t',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

  graph = Relation(
    name="Edge",
    dataframe=ratings)

  if build:
    db = Database.create(
      Config(num_threads=4),
      os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/db_pruned",
      [graph])
    db.build()
  db = Database.from_existing(os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/db_pruned")

  triangle_materialized(db)
  triangle_agg(db)
  four_clique_materialized(db)
  four_clique_agg(db)

def test_duplicated():
  build = False
  ratings = pd.read_csv(os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/facebook_duplicated.tsv",\
  sep='\t',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

  graph = Relation(
    name="Edge",
    dataframe=ratings)

  deg = pd.read_csv(os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/inv_degree.tsv",\
  sep='\t',\
  names=["0","a_0"],\
  dtype={"0":np.uint32,"a_0":np.float32})

  inv_degree = Relation(
    name="InvDegree",
    dataframe=deg)

  if build:
    db = Database.create(
      Config(num_threads=4),
      os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/db_duplicated",
      [graph,inv_degree])
    db.build()
  db = Database.from_existing(os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/db_duplicated")

  lollipop_agg(db)
  barbell_agg(db)
  four_clique_agg_sel(db)
  four_clique_sel(db)
  barbell_agg_sel(db)
  barbell_sel(db)
  pagerank(db)
  sssp(db)

def test_simple():
  build = True
  ratings = pd.read_csv(os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/simple.tsv",\
  sep='\t',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

  graph = Relation(
    name="Edge",
    dataframe=ratings)

  if build:
    db = Database.create(
      Config(num_threads=4),
      os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/db_simple",
      [graph])
    db.build()
  db = Database.from_existing(os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/db_simple")

  lollipop_materialized(db)
  barbell_materialized(db)

#basically the main method down here.
start()
test_pruned()
test_duplicated()
test_simple()
stop()