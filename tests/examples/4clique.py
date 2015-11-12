import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/facebook/db_pruned")
  emptyheaded.query("Flique(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d).")

  numRows = emptyheaded.numRows("Flique")
  if numRows != 30004668L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(numRows))
  data = emptyheaded.fetchData("Flique")

  print data.iloc[0]
  if data.iloc[0][0]!= 9 and data.iloc[0][1] != 8 and data.iloc[0][2] != 7 and data.iloc[0][3] != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(data[0]))

if __name__ == "__main__": main()
