import numpy as np
import pandas as pd
from emptyheaded import *

triangle = \
"""
Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).
"""

triangle_agg = \
"""
TriangleAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),z:uint64<-[COUNT(*)].
"""

fourclique = \
"""
Flique(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d).
"""

fourclique_agg = \
"""
FliqueAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),z:uint64<-[COUNT(*)].
"""

fourclique_sel_agg = \
"""
FliqueSelAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=0,z:uint64<-[COUNT(*)].
"""

fourclique_sel = \
"""
FliqueSel(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d),Edge(a,x),x=0.
"""

barbell = \
"""
Barbell(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z).
"""

barbell_agg = \
"""
BarbellAgg(;w) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z),w:uint64<-[COUNT(*)].
"""

barbell_sel = \
"""
BarbellSel(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,p),Edge(p,x),Edge(x,y),Edge(y,z),Edge(x,z),p=0.
"""

barbell_sel_agg = \
"""
BarbellSelAgg(;w) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,p),Edge(p,x),Edge(x,y),Edge(y,z),Edge(x,z),p=0,w:uint64<-[COUNT(*)]..
"""

lollipop = \
"""
Lollipop(a,b,c,x) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x).
"""

lollipop_agg = \
"""
LollipopAgg(;z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),z:uint64<-[COUNT(*)].
"""

pagerank = \
"""
N(;w) :- Edge(x,y),w:uint64<-[SUM(x;1)].
PageRank(x;y) :- Edge(x,z),y:float32<-[(1.0/N)].
PageRank(x;y)*[i=5]:-Edge(x,z),PageRank(z),InvDegree(z),y:float32 <- [0.15+0.85*SUM(z;1.0)].
"""

start()
ratings = pd.read_csv('test.csv',\
  sep=',',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

graph = Relation(
  name="Edge",
  dataframe=ratings)

#db = Database.create(
#  Config(),
#  "/Users/caberger/Documents/Research/code/EmptyHeaded/python/db",
#  [graph])
#db.build()

db = Database.from_existing("/Users/caberger/Documents/Research/code/EmptyHeaded/python/db")

print "TRIANGLE"
#db.eval(triangle)

print "4CLIQUE"
#db.eval(fourclique)

print "TRIANGLE AGG"
#db.eval(triangle_agg)

print "LOLLIPOP AGG"
db.eval(lollipop_agg)

print "BARBELL AGG"
db.eval(barbell_agg)

#db.eval(pagerank)

comm="""
g = db.get("Edge")
print g.annotated
print g.num_rows
print g.num_columns

db.load("Edge")
print g.getDF()

#db.save("graph")

ir = datalog(triangle_agg).ir 

for rule in ir.rules:
  print rule
"""
stop()
