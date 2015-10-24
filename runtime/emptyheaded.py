import json
import codegenerator.createDB
import codegenerator.env
import codegenerator.fetchRelation
import subprocess
import codegenerator.cppgenerator as cppgenerator
import codegenerator.cppexecutor as cppexecutor

environment = codegenerator.env.Environment()

def query(datalog_string):
  print subprocess.Popen("target/start -c config/config.json \"%s\"" % (datalog_string), cwd='../query_compiler' ,shell=True, stdout=subprocess.PIPE).stdout.read()
  cppgenerator.compileC("cgen")
  result = cppexecutor.execute("cgen",environment.config["memory"],["long","long","long"],"void*")
  print result[0].num_rows(result[1])
  print result[0].fetch_data(result[1])
  
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
	db_config="/Users/caberger/Documents/Research/data/databases/higgs/config.json"
	#db_config="/Users/caberger/Documents/Research/data/databases/facebook/config_pruned.json"

	#db_config="/afs/cs.stanford.edu/u/caberger/config_pruned.json"
	#createDB(db_config)
	loadDB("/Users/caberger/Documents/Research/data/databases/higgs/db_pruned/config.json")
	query("Triangle(a,b,c) :- R(a,b),R(b,c),R(a,c).")
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
