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

def test_pruned():
  ratings = pd.read_csv(os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/facebook_pruned.tsv",\
  sep='\t',\
  names=["0","1"],\
  dtype={"0":np.uint32,"1":np.uint32})

  graph = Relation(
    name="Edge",
    dataframe=ratings)

  config = Config()
  config.num_threads = 4

  db = Database.create(
    config,
    os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/db_pruned",
    [graph])
  db.build()

  db = Database.from_existing(os.path.expandvars("$EMPTYHEADED_HOME")+"/examples/graph/data/db_pruned")

  print "\nTRIANGLE"
  db.eval(triangle)

  tri = db.get("Triangle")
  print tri.annotated
  print tri.num_rows
  print tri.num_columns
  df = tri.getDF()

  if tri.num_rows != 1612010L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(numRows))
  row0 = df.iloc[0]
  if row0[0] != 6l or row0[1] != 5l or row0[2]!=2l: #(6l,5l,2l)
    raise ResultError("ROW0 INCORRECT: " + str(row0))

start()
test_pruned()
stop()