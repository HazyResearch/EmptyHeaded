import emptyheaded 

def main():
	db_config="$EMPTYHEADED_HOME/examples/graph/data/facebook/config_pruned.json"
	emptyheaded.createDB(db_config)
	emptyheaded.loadDB("$EMPTYHEADED_HOME/examples/graph/data/facebook/db_pruned")
	emptyheaded.query("Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).")

	numRows = emptyheaded.numRows("Triangle")
	if numRows != 0L:#1612010L
		raise ResultError("NUMBER OF ROWS INCORRECT: " + numRows)
	print emptyheaded.fetchData("Triangle")[0]

if __name__ == "__main__": main()
