import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/rdf/data/lubm1/db")
  #emptyheaded.loadDB("/Users/caberger/Documents/Research/data/lubm1000/db")

  emptyheaded.query("lubm2(a,b,c) :- memberOf(a,b),subOrganizationOf(b,c),undegraduateDegreeFrom(a,c),rdftype(a,x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent'),rdftype(b,y='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department'),rdftype(c,z='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University').")
  data = emptyheaded.fetchData("lubm2")
  print len(data)
  if len(data) != 0:
    raise ResultError("ROW0 INCORRECT: " + str(data))

if __name__ == "__main__": main()
