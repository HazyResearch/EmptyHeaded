import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/rdf/data/lubm1/db")
  emptyheaded.query("lubm8(a,b,c) :- memberOf(a,b),emailAddress(a,c),rdftype(a,d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent'),subOrganizationOf(b,e='http://www.University0.edu'),rdftype(b,f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department').")

  data = emptyheaded.fetchData("lubm8")
  print len(data)
  print data.iloc[50][1]
  if len(data) != 5916 or data.iloc[50][1] != "UndergraduateStudent143@Department0.University0.edu":
    raise ResultError("ROW INCORRECT: " + str(data.iloc[50]))

if __name__ == "__main__": main()
