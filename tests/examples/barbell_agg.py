import emptyheaded 

class ResultError(Exception):
    pass

def main():
	emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/facebook/db_pruned")
	emptyheaded.query("Barbell(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z);m=<COUNT(*)>.")

	numRows = emptyheaded.numRows("Barbell")
	if numRows != 113389128L:
		raise ResultError("NUMBER OF ROWS INCORRECT: " + str(numRows))

if __name__ == "__main__": main()
