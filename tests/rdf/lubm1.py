import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/rdf/data/lubm1/db")
  emptyheaded.query("lubm1(a) :- takesCourse(a,b='http://www.Department0.University0.edu/GraduateCourse0'),rdftype(a,c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent').")
  data = emptyheaded.fetchData("lubm1")
  print len(data)
  print data.iloc[3][0]
  if len(data) != 4 or data.iloc[3][0] != "http://www.Department0.University0.edu/GraduateStudent44":
    raise ResultError("ROW0 INCORRECT: " + str(data))

if __name__ == "__main__": main()
