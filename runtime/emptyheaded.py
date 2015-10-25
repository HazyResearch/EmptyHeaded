import json
import codegenerator.createDB
import codegenerator.env
import codegenerator.fetchRelation
import subprocess
import codegenerator.cppgenerator as cppgenerator
import codegenerator.cppexecutor as cppexecutor

environment = codegenerator.env.Environment()

def query(datalog_string):
  print environment.config["database"]
  print subprocess.Popen("target/start -c %s/config.json \"%s\"" % (environment.config["database"],datalog_string), cwd='../query_compiler' ,shell=True, stdout=subprocess.PIPE).stdout.read()
  environment.fromJSON(environment.config["database"]+"/config.json")
  cppgenerator.compileC("Query")
  schema = environment.schemas[environment.config["resultName"]]
  eTypes = map(lambda i:str(schema["attributes"][i]["attrType"]),environment.config["resultOrdering"])
  result = cppexecutor.execute("Query",environment.config["memory"],eTypes,schema["annotation"])
  print result[0].num_rows(result[1])
  #print result[0].fetch_data(result[1])

def compileQuery(datalog_string):
  print subprocess.Popen("target/start DunceCap.QueryPlanner %s \"%s\"" % (QUERY_COMPILER_CONFIG_DIR, datalog_string), cwd='../query_compiler' ,shell=True, stdout=subprocess.PIPE).stdout.read()

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
	#db_config="/Users/caberger/Documents/Research/data/databases/higgs/config.json"
	#db_config="/Users/caberger/Documents/Research/data/databases/facebook/config_pruned.json"

	#db_config="../examples/graph/data/facebook/config_pruned.json"
	#createDB(db_config)
	loadDB("../examples/graph/data/facebook/db_pruned/config.json")
	query("Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).")
	#print fetchData("R")
	#print numRows("R")

	#loadDB("/Users/caberger/Documents/Research/data/databases/simple/db/config.json")
	com="""
	compileQuery("query1")
 	q1 = loadQuery("query1")
 	q1_result = q1.run()
 	print q1.num_rows(q1_result)
 	q1.fetch_data(q1_result)
 	"""


if __name__ == "__main__": main()
