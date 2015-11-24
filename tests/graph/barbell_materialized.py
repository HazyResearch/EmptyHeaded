import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/simple/db")
  emptyheaded.query("Barbell(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z).")

  row0 = emptyheaded.fetchData("Barbell").iloc[55]
  print row0
  if row0[0] != 5l or row0[1] != 4l or row0[2] != 4l or row0[3] != 3l or row0[4] != 5l or row0[5] != 3l: #5  4  4  3  5  3
    raise ResultError("ROW0 INCORRECT: " + str(row0))

if __name__ == "__main__": main()
