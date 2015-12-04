import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/rdf/data/lubm1/db")
  emptyheaded.query("lubm4(a,b,c,d) :- worksFor(a,e='http://www.Department0.University0.edu'),name(a,b),emailAddress(a,d),telephone(a,c),rdftype(a,f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor').")

  data = emptyheaded.fetchData("lubm4")
  print len(data)
  print data.iloc[13][0]
  if len(data) != 14 or data.iloc[13][0] != "http://www.Department0.University0.edu/AssociateProfessor9":
    raise ResultError("ROW0 INCORRECT: " + str(row0))

if __name__ == "__main__": main()
