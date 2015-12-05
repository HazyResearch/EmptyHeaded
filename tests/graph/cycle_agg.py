import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/facebook/db")
  # this specifically tests that it's okay for us to write Edge(a,e)
  # when the edges are undirected (if they were directed, we'd have to write e, a)
  emptyheaded.query("Cycle5(;m:long) :- Edge(a,b),Edge(b,c),Edge(c,d),Edge(d,e),Edge(a,e);m=[<<COUNT(*)>>].")

  data = emptyheaded.fetchData("Cycle5")
  print data.iloc[0][0]
  if data.iloc[0][0] != 163853203160:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(data[0]))

if __name__ == "__main__": main()
