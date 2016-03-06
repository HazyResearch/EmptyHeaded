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
  print "\nLollipop AGG"
  db.eval(lolli_agg)

  tri = db.get("LollipopAgg")
  df = tri.getDF()
  print df

  if tri.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

  if df.iloc[0][0] != 1426911480L:
    raise ResultError("ANNOTATION INCORRECT: " + str(df.iloc[0][0]))

def test_lubm():
  build = True

  takesCourse = Relation(
    name="takesCourse",
    schema=Schema(attributes=["string","string"]),
    filename=os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/data/takesCourse.tsv")

  if build:
    db = Database.create(
      Config(num_threads=4),
      os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/databases/db_lubm1",
      [takesCourse])
    db.build()
  db = Database.from_existing(os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/databases/db_lubm1")

#basically the main method down here.
start()
test_lubm()
stop()