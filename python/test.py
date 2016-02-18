import numpy as np
import pandas as pd
from emptyheaded import *

start()
ratings = pd.read_csv('test.csv',\
  sep=',',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

graph = Relation(
  name="graph",
  dataframe=ratings)

#db = Database.create(
#  Config(),
#  "db",
#  [graph])
#db.build()

db = Database.from_existing("db")

rule = RULE(
  RESULT(RELATION(name="Triangle",attributes=["a","b","c"])),
  RECURSION(),
  OPERATION(operation="*"),
  ORDER(attributes=["a","b","c"]),
  PROJECT(attributes=[]),
  JOIN([
    RELATION(name="R",attributes=["a","b"]),
    RELATION(name="R",attributes=["b","c"]),
    RELATION(name="R",attributes=["a","c"])]),
  AGGREGATES([]),
  FILTERS([])
)

ir = IR([rule])
jir = ir.python2java()
ir = IR.java2python(jir)
print ir.rules[0]

db.optimize(ir)

stop()