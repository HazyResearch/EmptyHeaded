import json
import codegenerator.createDB
import codegenerator.env
import codegenerator.fetchRelation

environment = codegenerator.env.Environment()

def createDB(name):
	codegenerator.createDB.fromJSON(name,environment)
	#environment.dump()

def fetchData(relation):
	return codegenerator.fetchRelation.fetch(relation,environment)

def numRows(relation):
	return codegenerator.fetchRelation.numRows(relation,environment)

def saveDB():
	environment.toJSON(environment.config["database"]+"/config.json")

def loadDB(path):
	environment.fromJSON(path)

def main():
	db_config="/Users/caberger/Documents/Research/data/databases/higgs/config_pruned.json"
	#db_config="/Users/caberger/Documents/Research/data/databases/simple/config.json"

	#db_config="/afs/cs.stanford.edu/u/caberger/config_pruned.json"
	createDB(db_config)
	#loadDB("/afs/cs.stanford.edu/u/caberger/db/config.json")
	print fetchData("R")
	print numRows("R")

	#loadDB("/Users/caberger/Documents/Research/data/databases/simple/db/config.json")
	com="""
	compileQuery("query1")
 	q1 = loadQuery("query1")
 	q1_result = q1.run()
 	print q1.num_rows(q1_result)
 	q1.fetch_data(q1_result)
 	"""


if __name__ == "__main__": main()
