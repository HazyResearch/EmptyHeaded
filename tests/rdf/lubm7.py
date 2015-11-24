import emptyheaded 

class ResultError(Exception):
    pass

def main():
  emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/rdf/data/lubm1/db")
  emptyheaded.query("lubm7(a,b) :- teacherOf(c='http://www.Department0.University0.edu/AssociateProfessor0',b),takesCourse(a,b),type(b,d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course'),type(a,e='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent').")

  data = emptyheaded.fetchData("lubm7")
  print len(data)
  print data.iloc[50][0]
  if len(data) != 59 or data.iloc[50][0] != "http://www.Department0.University0.edu/UndergraduateStudent505":
    raise ResultError("ROW INCORRECT: " + str(data.iloc[50]))

if __name__ == "__main__": main()
