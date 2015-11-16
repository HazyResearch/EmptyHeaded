import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/simple/db")
  emptyheaded.query("Lollipop(a,b,c,x) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x).")

  row0 = emptyheaded.fetchData("Lollipop").iloc[27]
  if row0[0] != 5l or row0[1] != 4l or row0[2] != 3l or row0[3] != 4l:
    raise ResultError("ROW0 INCORRECT: " + str(row0))

if __name__ == "__main__": main()
