import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/facebook/db")
  emptyheaded.query("Lollipop(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x);m=<<COUNT(*)>>.")

  data = emptyheaded.fetchData("Lollipop")
  print data.iloc[0][0]
  if data.iloc[0][0] != 1426911480L:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(data[0]))

if __name__ == "__main__": main()
