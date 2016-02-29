import numpy as np
import pandas as pd
from emptyheaded import *

def triangle():
  return datalog("""
    Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).
  """).ir

def triangle_counting():
  return datalog("""
    Triangle(a;z) :- Edge(a,b),Edge(b,c),Edge(a,c),z:uint64<-[COUNT(b,c)].
  """).ir

def triangle_agg():
  return datalog("""
    TriangleAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),z:uint64<-[COUNT(*)].
  """).ir

def fourclique():
  return datalog("""
    Flique(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d).
    """).ir

def fourclique_agg():
  return datalog("""
    FliqueAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),z:uint64<-[COUNT(*)].
  """).ir


def fourclique_sel_agg():
  return datalog("""
    FliqueSelAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=0,z:uint64<-[COUNT(*)].
  """).ir

def fourclique_sel():
  return datalog("""
    FliqueSel(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=0.
  """).ir

def pagerank():
  return datalog("""
    N(;w) :- Edge(x,y),w:uint64<-[SUM(x;1)].
    PageRank(x;y) :- Edge(x,z),y:float32<-[(1.0/N)].
    PageRank(x;y)*[i=5]:-Edge(x,z),PageRank(z),InvDegree(z),y:float32 <- [0.15+0.85*SUM(z;1.0)].
  """).ir

def lubm1():
  return datalog("""
    lubm1(a) :-
      takesCourse(a,b),
      b='http://www.Department0.University0.edu/GraduateCourse0',
      rdftype(a,c),
      c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent'.
  """).ir

####Main method beloe
start() #spin up the JVM

queries = {
  "triangle":triangle,
  "triangle_counting":triangle_counting,
  "pagerank":pagerank,
  "lubm1":lubm1,
  "triangle_agg":triangle_agg,
  "fourclique":fourclique,
  "fourclique_agg":fourclique_agg,
  "fourclique_sel":fourclique_sel,
  "fourclique_sel_agg":fourclique_sel_agg
}

for query in queries:
  print "\n\n"+query
  ir = queries[query]()
  for rule in ir.rules:
    print rule


stop() #tear down the JVM
