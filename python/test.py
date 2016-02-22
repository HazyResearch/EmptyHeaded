import numpy as np
import pandas as pd
from emptyheaded import *

def triangle():
  return datalog("""
    Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).
  """).ir

start()
ratings = pd.read_csv('test.csv',\
  sep=',',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

graph = Relation(
  name="graph",
  dataframe=ratings)

db = Database.create(
  Config(),
  "/dfs/scratch0/caberger/systems/eh-2.0/EmptyHeaded/python/db",
  [graph])
db.build()

db = Database.from_existing("db")

g = db.get("graph")
print g.annotated
print g.num_rows
print g.num_columns

db.load("graph")
print g.getDF()

#db.save("graph")

ir = triangle() 

for rule in ir.rules:
  print rule


stop()
