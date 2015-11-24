import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/rdf/data/lubm1/db")
  emptyheaded.query("lubm6(a) :- type(a,b='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent').")

  data = emptyheaded.fetchData("lubm6")
  print len(data)
  print data.iloc[0][0]
  if len(data) != 5916 or data.iloc[0][0] != "http://www.Department0.University0.edu/UndergraduateStudent0":
    raise ResultError("ROW0 INCORRECT: " + str(row0))

if __name__ == "__main__": main()
