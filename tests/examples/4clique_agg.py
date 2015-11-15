import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/facebook/db_pruned")
  emptyheaded.query("Flique(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d);m=<<COUNT(*)>>.")

  data = emptyheaded.fetchData("Flique")
  if data.iloc[0][0]!= 30004668L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(data[0]))

if __name__ == "__main__": main()
