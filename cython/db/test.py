import numpy as np
import pandas as pd
from DB import DB

db = DB()
db.load("fake")

#code generates cython wrapper around the trie
#potentially loads the trie into RAM
#c++ class takes map & loads if nesc
#g = db.load("graph") 

#returns a dataframe
#g.dataFrame()

#compiles query & runs (accpets the map and updates it w relations
#that are loaded and the result)
#db.query("Triangle <- R(a,b),R(b,c),R(a,c).")

# master (sits inside of the database class)
#    -> holds map from string names live relations
#    -> dynamically runs queries
#    -> dynamically runs loads