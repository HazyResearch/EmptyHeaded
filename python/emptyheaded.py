import os
import codegenerator.createDB
import codegenerator.env
import codegenerator.fetchRelation

environment = codegenerator.env.Environment()

def createDB(name):
	codegenerator.createDB.fromJSON(name,environment)
	#environment.dump()

def fetchData(relation):
	codegenerator.fetchRelation.fetch(relation,environment)

def main():
	#db_config="/Users/caberger/Documents/Research/data/databases/higgs/config_pruned.json"
	#db_config="/Users/caberger/Documents/Research/data/databases/simple/config.json"

	fetchData("R")
	#createDB(db_config)
	com="""
	compileQuery("query1")
 	q1 = loadQuery("query1")
 	q1_result = q1.run()
 	print q1.num_rows(q1_result)
 	q1.fetch_data(q1_result)
 	"""


if __name__ == "__main__": main()