import numpy as np
import pandas as pd
from emptyheaded import *

class ResultError(Exception):
    pass

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


def four_clique_agg_sel(db):
  fourclique_sel_agg = \
"""
FliqueSelAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=0,z:long<-[COUNT(*)].
"""
  print "4 CLIQUE SELECTION AGG"
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
  print "4 CLIQUE SELECTION"
  db.eval(fourclique_sel)

  foursel = db.get("FliqueSel")
  df = foursel.getDF()

  if foursel.num_rows != 3759972L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))
  row0 = df.iloc[0]
  if row0[0] != 1l or row0[1] != 0l or row0[2] != 48l or row0[3]!=53l: #(6l,5l,2l)
    raise ResultError("ROW0 INCORRECT: " + str(row0))

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

def test_duplicated():
  build = True
  ratings = pd.read_csv(os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/facebook_duplicated.tsv",\
  sep='\t',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

  graph = Relation(
    name="Edge",
    dataframe=ratings)

  if build:
    db = Database.create(
      Config(num_threads=4),
      os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/db_duplicated",
      [graph])
    db.build()
  db = Database.from_existing(os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/db_duplicated")

  four_clique_agg_sel(db)
  four_clique_sel(db)

start()
test_pruned()
test_duplicated()
stop()