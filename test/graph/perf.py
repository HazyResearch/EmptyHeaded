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
  print "\nQUERY: LOLLIPOP AGG"
  db.eval(lolli_agg)

def barbell_agg(db):
  b_agg = \
"""
BarbellAgg(;w) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z),w:long<-[COUNT(*)].
"""
  print "\nQUERY: BARBELL AGG"
  db.eval(b_agg)

def barbell_materialized(db):
  barbell = \
"""
Barbell(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z).
"""
  print "\nQUERY: BARBELL"
  db.eval(barbell)

def lollipop_materialized(db):
  lollipop = \
"""
Lollipop(a,b,c,x) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x).
"""
  print "\nQUERY: LOLLIPOP"
  db.eval(lollipop)

def triangle_agg(db):
  tri_agg = \
"""
TriangleAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),z:long<-[COUNT(*)].
"""
  print "\nQUERY: TRIANGLE AGG"
  db.eval(tri_agg)

def four_clique_agg(db):
  flique = \
"""
FliqueAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),z:long<-[COUNT(*)].
"""
  print "\nQUERY: FOUR CLIQUE AGG"
  db.eval(flique)

def triangle_materialized(db):
  triangle = \
"""
Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).
"""
  print "\nQUERY: TRIANGLE"
  db.eval(triangle)

def four_clique_materialized(db):
  four_clique = \
"""
Flique(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d).
"""
  print "\nQUERY: FOUR CLIQUE"
  db.eval(four_clique)

def four_clique_agg_sel(db,node):
  fourclique_sel_agg = \
"""
FliqueSelAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=%(node)s,z:long<-[COUNT(*)].
"""% locals()
  print "\nQUERY: 4 CLIQUE SELECTION AGG"
  db.eval(fourclique_sel_agg)

def four_clique_sel(db,node):
  fourclique_sel = \
"""
FliqueSel(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=%(node)s.
"""% locals()
  print "\nQUERY: 4 CLIQUE SELECTION"
  db.eval(fourclique_sel)

def barbell_agg_sel(db,node):
  barbell_sel_agg = \
"""
BarbellSelAgg(;w) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,p),Edge(p,x),Edge(x,y),Edge(y,z),Edge(x,z),p=%(node)s,w:long<-[COUNT(*)].
"""% locals()
  print "\nQUERY: BARBELL SELECTION AGG"
  db.eval(barbell_sel_agg)

def barbell_sel(db,node):
  barbell_s = \
"""
BarbellSel(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,p),Edge(p,x),Edge(x,y),Edge(y,z),Edge(x,z),p=%(node)s.
"""% locals()
  print "\nQUERY: BARBELL SELECTION"
  db.eval(barbell_s)

def pagerank(db):
  pr="""
N(;w) :- Edge(x,y),w:long<-[SUM(x;1)].
PageRank(x;y) :- Edge(x,z),y:float<-[(1.0/N)].
PageRank(x;y)*[i=5]:-Edge(x,z),PageRank(z),InvDegree(z),y:float <- [0.15+0.85*SUM(z;1.0)].
"""
  print "\nQUERY: PAGERANK"
  db.eval(pr)

def sssp(db,node):
  paths = \
"""
SSSP(x;y) :- Edge(w,x),w=%(node)s,y:long <- [1].
SSSP(x;y)*[c=0] :- Edge(w,x),SSSP(w),y:long <- [1+MIN(w;1)].
"""% locals()
  print "\nQUERY: SSSP"
  db.eval(paths)

def test_pruned(dataset):
  build = False
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
  if dataset != "twitter2010":
    four_clique_agg(db)

def test_duplicated(dataset):
  build = True
  ratings = pd.read_csv("/dfs/scratch0/caberger/datasets/eh_datasets/"+dataset+"/duplicated/data.tsv",\
  sep='\t',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

  deg = pd.read_csv("/dfs/scratch0/caberger/datasets/eh_datasets/"+dataset+"/pagerank/inverse_degree.tsv",\
  sep='\t',\
  names=["0","a_0"],\
  dtype={"0":np.uint32,"a_0":np.float32})

  graph = Relation(
    name="Edge",
    dataframe=ratings)

  inv_degree = Relation(
    name="InvDegree",
    dataframe=deg)

  if build:
    db = Database.create(
      Config(num_threads=56),
      "/dfs/scratch0/caberger/datasets/eh_datasets/databases/"+dataset+"/db_duplicated",
      [graph,inv_degree])
    db.build()
  db = Database.from_existing("/dfs/scratch0/caberger/datasets/eh_datasets/databases/"+dataset+"/db_duplicated")

  lollipop_agg(db)
  barbell_agg(db)

  node_ids = {
    "googlePlus": {
      "high":"209",
      "low":"555",
      "paths":"6966"
    },
    "higgs": {
      "high":"5153",
      "low":"34",
      "paths":"83222"
    },
    "socLivejournal": {
      "high":"10010",
      "low":"57",
      "paths":"10009"
    },
    "orkut": {
      "high":"43609",
      "low":"78",
      "paths":"43608"
    },
    "cidPatents": {
      "high":"4723130",
      "low":"33156",
      "paths":"5795784"
    },
    "twitter2010": {
      "paths":"1037948"
    }
  }

  if dataset != "twitter2010":
    four_clique_agg_sel(db,node_ids[dataset]["high"])
    four_clique_agg_sel(db,node_ids[dataset]["low"])
    barbell_agg_sel(db,node_ids[dataset]["high"])
    barbell_agg_sel(db,node_ids[dataset]["low"])

  pagerank(db)
  sssp(db,node_ids[dataset]["paths"])

#basically the main method down here.
start()
#datasets = ["googlePlus","higgs","socLivejournal","orkut","cidPatents","twitter2010"]
datasets = ["twitter2010"]

for dataset in datasets:
  print "DATASET: " + dataset
  test_pruned(dataset)
  test_duplicated(dataset)
stop()