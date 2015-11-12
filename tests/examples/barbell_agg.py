import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/facebook/db")
  emptyheaded.query("Barbell(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z);m=<<COUNT(*)>>.")
  data = emptyheaded.fetchData("Barbell")
  print data.iloc[0][0]
  if data.iloc[0][0] != 20371831447136L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(data))

if __name__ == "__main__": main()
