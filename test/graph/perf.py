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
  print "\nLOLLIPOP AGG"
  db.eval(lolli_agg)

def barbell_agg(db):
  b_agg = \
"""
BarbellAgg(;w) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z),w:long<-[COUNT(*)].
"""
  print "\nBARBELL AGG"
  db.eval(b_agg)

def barbell_materialized(db):
  barbell = \
"""
Barbell(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z).
"""
  print "\nBARBELL"
  db.eval(barbell)

def lollipop_materialized(db):
  lollipop = \
"""
Lollipop(a,b,c,x) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x).
"""
  print "\nLOLLIPOP"
  db.eval(lollipop)

def triangle_agg(db):
  tri_agg = \
"""
TriangleAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),z:long<-[COUNT(*)].
"""
  print "\nTRIANGLE AGG"
  db.eval(tri_agg)

def four_clique_agg(db):
  flique = \
"""
FliqueAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),z:long<-[COUNT(*)].
"""
  print "\nFOUR CLIQUE AGG"
  db.eval(flique)

def triangle_materialized(db):
  triangle = \
"""
Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).
"""
  print "\nTRIANGLE"
  db.eval(triangle)

def four_clique_materialized(db):
  four_clique = \
"""
Flique(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d).
"""
  print "\nFOUR CLIQUE"
  db.eval(four_clique)

def four_clique_agg_sel(db):
  fourclique_sel_agg = \
"""
FliqueSelAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=0,z:long<-[COUNT(*)].
"""
  print "\n4 CLIQUE SELECTION AGG"
  db.eval(fourclique_sel_agg)

def four_clique_sel(db):
  fourclique_sel = \
"""
FliqueSel(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=0.
"""
  print "\n4 CLIQUE SELECTION"
  db.eval(fourclique_sel)

def barbell_agg_sel(db):
  barbell_sel_agg = \
"""
BarbellSelAgg(;w) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,p),Edge(p,x),Edge(x,y),Edge(y,z),Edge(x,z),p=6,w:long<-[COUNT(*)].
"""
  print "\nBARBELL SELECTION AGG"
  db.eval(barbell_sel_agg)

def barbell_sel(db):
  barbell_s = \
"""
BarbellSel(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,p),Edge(p,x),Edge(x,y),Edge(y,z),Edge(x,z),p=6.
"""
  print "\nBARBELL SELECTION"
  db.eval(barbell_s)

def test_pruned(dataset):
  build = True
  ratings = pd.read_csv("/dfs/scratch0/caberger/datasets/eh_datasets/"+dataset+"/pruned/data.tsv",\
  sep='\t',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

  graph = Relation(
    name="Edge",
    dataframe=ratings)

  if build:
    db = Database.create(
      Config(num_threads=56),
      "/dfs/scratch0/caberger/datasets/eh_datasets/databases/"+dataset+"/db_pruned",
      [graph])
    db.build()
  db = Database.from_existing("/dfs/scratch0/caberger/datasets/eh_datasets/databases/"+dataset+"/db_pruned")

  triangle_agg(db)
  triangle_materialized(db)
  #four_clique_materialized(db)
  four_clique_agg(db)

def test_duplicated(dataset):
  build = True
  ratings = pd.read_csv("/dfs/scratch0/caberger/datasets/eh_datasets/"+dataset+"/duplicated/data.tsv",\
  sep='\t',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

  graph = Relation(
    name="Edge",
    dataframe=ratings)

  if build:
    db = Database.create(
      Config(num_threads=56),
      "/dfs/scratch0/caberger/datasets/eh_datasets/databases/"+dataset+"/db_duplicated",
      [graph])
    db.build()
  db = Database.from_existing("/dfs/scratch0/caberger/datasets/eh_datasets/databases/"+dataset+"/db_duplicated")

  lollipop_agg(db)
  barbell_agg(db)
  #four_clique_agg_sel(db)
  #barbell_agg_sel(db)

#basically the main method down here.
start()
#datasets = ["googlePlus","higgs","socLivejournal","orkut","cidPatents","twitter2010"]
datasets = ["twitter2010"]

for dataset in datasets:
  print "DATASET: " + dataset
  #test_pruned(dataset)
  test_duplicated(dataset)
stop()