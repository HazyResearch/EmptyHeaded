import numpy as np
import pandas as pd
from emptyheaded import *

triangle = \
  """
    Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).
  """

triangle_agg = \
"""
Triangle(a;z) :- Edge(a,b),Edge(b,c),Edge(a,c),z:uint64<-[COUNT(*)].
"""

start()
ratings = pd.read_csv('test.csv',\
  sep=',',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

graph = Relation(
  name="Edge",
  dataframe=ratings)

db = Database.create(
  Config(),
  "db",
  [graph])
db.build()

db = Database.from_existing("/Users/caberger/Documents/Research/code/EmptyHeaded/python/db")

db.generate(triangle)

#g = db.get("graph")
#print g.annotated
#print g.num_rows
#print g.num_columns

#db.load("graph")
#print g.getDF()

#db.save("graph")

ir = datalog(triangle_agg).ir 

for rule in ir.rules:
  print rule

stop()
