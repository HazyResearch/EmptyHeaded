import emptyheaded 

class ResultError(Exception):
    pass

def main():
	db_config="$EMPTYHEADED_HOME/examples/graph/data/facebook/config_pruned.json"
	emptyheaded.createDB(db_config)
	emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/facebook/db_pruned")
	emptyheaded.query("Lollipop(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x);m=<COUNT(*)>.")

	numRows = emptyheaded.numRows("Lollipop")
	if numRows != 113389128L:
		raise ResultError("NUMBER OF ROWS INCORRECT: " + str(numRows))

if __name__ == "__main__": main()
