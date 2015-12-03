import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/facebook/db")
  emptyheaded.query(
  """SSSP(x;y:int) :- Edge(w=0,x);y=[<<CONST(w;1)>>].
    SSSP(x;y:int)*[c=0] :- Edge(w,x),SSSP(w);y=[1+<<MIN(w;1)>>].""")
  data = emptyheaded.fetchData("SSSP")
  print data
  if data.iloc[1000]["annotation"] != 2:
    raise ResultError("SSSP value incorrect: " + str(data.iloc[1000]["annotation"]))

if __name__ == "__main__": main()
