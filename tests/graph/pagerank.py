import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/facebook/db")
  emptyheaded.query(
  """N(;w:int):-Edge(x,y);w=[<<COUNT(x)>>].
     PageRank(x;y:float):-Edge(x,z);y=[(1.0/N)*<<CONST(z;1.0)>>].
     PageRank (x;y:float)*[i=5]:-Edge(x,z),PageRank(z),InvDegree(z);y=[0.15+0.85*<<SUM(z;1.0)>>].""")
  data = emptyheaded.fetchData("PageRank")
  print data
  if (data.iloc[0]["annotation"]-15.227079960463206) > 0.0001:
    raise ResultError("PageRank value incorrect: " + str(data.iloc[0]["annotation"]))

if __name__ == "__main__": main()
