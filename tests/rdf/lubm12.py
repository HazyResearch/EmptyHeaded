import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/rdf/data/lubm1/db")
  emptyheaded.query("lubm12(a,b) :- worksFor(a,b),type(a,c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor'),subOrganizationOf(b,d='http://www.University0.edu'),type(b,e='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department').")

  data = emptyheaded.fetchData("lubm12")
  print len(data)
  print data.iloc[100][1]
  if len(data) != 125 or data.iloc[100][1] != "http://www.Department6.University0.edu":
    raise ResultError("ROW INCORRECT: " + str(data.iloc[100]))

if __name__ == "__main__": main()
