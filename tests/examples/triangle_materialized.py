import emptyheaded 

def main():
	db_config="$EMPTYHEADED_HOME/examples/graph/data/facebook/config_pruned.json"
	createDB(db_config)
	loadDB("$EMPTYHEADED_HOME/examples/graph/data/facebook/db_pruned")
	query("Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).")

	print numRows("Triangle")
	print fetchData("Triangle")[0]

if __name__ == "__main__": main()
