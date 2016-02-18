import numpy as np
import pandas as pd
from emptyheaded import *

def triangle():
  return datalog("""
    Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).
  """).ir

def triangle_counting():
  return datalog("""
    Triangle(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),z:uint64<-[COUNT(*)].
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
  "lubm1":lubm1
}

for query in queries:
  print "\n\n"+query
  ir = queries[query]() 
  for rule in ir.rules:
    print rule


stop() #tear down the JVM