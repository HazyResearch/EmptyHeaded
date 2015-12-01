import pandas as pd
import json
import codegenerator.createDB
import codegenerator.env
import codegenerator.fetchRelation
import subprocess
import codegenerator.cppgenerator as cppgenerator
import codegenerator.cppexecutor as cppexecutor
import os
import re

hashindex = 0
environment = codegenerator.env.Environment()


def query(datalog_string):
  global hashindex
  qcpath = os.path.expandvars("$EMPTYHEADED_HOME")+"/query_compiler/"
  mydir=os.getcwd()
  os.chdir(qcpath)
  QUERY_COMPILER_RUN_SCRIPT = os.path.expandvars("$EMPTYHEADED_HOME")+"/query_compiler/target/pack/bin/query-compiler"
  command = QUERY_COMPILER_RUN_SCRIPT+" -c "+environment.config["database"]+"/config.json -q \""+ re.escape(datalog_string) + "\""
  os.system(command)  
  os.chdir(mydir)
  environment.fromJSON(environment.config["database"]+"/config.json")
  cppgenerator.compileC(str(hashindex),str(environment.config["numThreads"]))
  schema = environment.schemas[environment.config["resultName"]]
  eTypes = map(lambda i:str(schema["attributes"][i]["attrType"]),environment.config["resultOrdering"])
  result = cppexecutor.execute(str(hashindex),environment.config["memory"],eTypes,schema["annotation"])
  relationResult = {}
  relationResult["query"] = result[0]
  relationResult["trie"] = result[1]
  relationResult["hash"] = hashindex
  environment.liverelations[environment.config["resultName"]] = relationResult
  hashindex += 1

def runQueryPlan(config):
  global hashindex
  qcpath = os.path.expandvars("$EMPTYHEADED_HOME")+"/query_compiler/"
  mydir=os.getcwd()
  os.chdir(qcpath)
  QUERY_COMPILER_RUN_SCRIPT = os.path.expandvars("$EMPTYHEADED_HOME")+"/query_compiler/target/pack/bin/query-compiler"
  command = QUERY_COMPILER_RUN_SCRIPT+" -c "+environment.config["database"]+"/config.json -g " + config
  os.system(command)  
  os.chdir(mydir)
  environment.fromJSON(environment.config["database"]+"/config.json")
  cppgenerator.compileC(str(hashindex),str(environment.config["numThreads"]))
  schema = environment.schemas[environment.config["resultName"]]
  eTypes = map(lambda i:str(schema["attributes"][i]["attrType"]),environment.config["resultOrdering"])
  result = cppexecutor.execute(str(hashindex),environment.config["memory"],eTypes,schema["annotation"])
  relationResult = {}
  relationResult["query"] = result[0]
  relationResult["trie"] = result[1]
  relationResult["hash"] = hashindex
  environment.liverelations[environment.config["resultName"]] = relationResult
  hashindex += 1

def compileQuery(datalog_string):
  print subprocess.Popen("target/start DunceCap.QueryPlanner %s \"%s\"" % (QUERY_COMPILER_CONFIG_DIR, datalog_string), cwd='../query_compiler' ,shell=True, stdout=subprocess.PIPE).stdout.read()

def createDB(name):
  name = os.path.expandvars(name)
  codegenerator.createDB.fromJSON(name,environment)
  #environment.dump()

def fetchData(relation):
  if relation in environment.liverelations:
    query = environment.liverelations[relation]
    schema = environment.schemas[relation]
    annotation = str(schema["annotation"])
    cols = map(str, schema["orderings"][0])
    if annotation != "void*":
      cols.append("annotation")
    tuples = eval("""query["query"].fetch_data_"""+str(query["hash"])+"""(query["trie"])""")
    return pd.DataFrame.from_records(data=tuples,columns=cols)
  else:
    schema = environment.schemas[relation]
    annotation = str(schema["annotation"])
    cols = map(str, schema["orderings"][0])
    if annotation != "void*":
      cols.append("annotation")
    fetchedData = codegenerator.fetchRelation.fetch(relation,environment)
    return pd.DataFrame.from_records(data=fetchedData,columns=cols)

def numRows(relation):
  if relation in environment.liverelations:
    query = environment.liverelations[relation]
    return eval("""query["query"].num_rows_"""+str(query["hash"])+"""(query["trie"])""")
  else:
    return codegenerator.fetchRelation.numRows(relation,environment)

def saveDB():
  environment.toJSON(environment.config["database"]+"/config.json")

def loadDB(path):
  path = os.path.expandvars(path)+"/config.json"
  environment.fromJSON(path)

def main():
  comm="""
  db_config="/afs/cs.stanford.edu/u/caberger/config_pruned.json"
  #createDB(db_config)
  loadDB("/dfs/scratch0/caberger/datasets/higgs/db_python_pruned")
  query("Triangle(;x:long) :- Edge(a,b),Edge(b,c),Edge(a,c);x=<<COUNT(*)>>.")
  query("Flique(;x:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d);x=<<COUNT(*)>>.")

  db_config="/afs/cs.stanford.edu/u/caberger/config.json"
  #createDB(db_config)
  loadDB("/dfs/scratch0/caberger/datasets/higgs/db_python")
  query("Lollipop(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x);m=<<COUNT(*)>>.")
  query("Barbell(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z);m=<<COUNT(*)>>.")
  """

  db_config="/afs/cs.stanford.edu/u/caberger/rdf.json"
  createDB(db_config)
  loadDB("/dfs/scratch0/caberger/datasets/lubm10000/db_python")
  #query("Triangle(;x:long) :- Edge(a,b),Edge(b,c),Edge(a,c);x=<<COUNT(*)>>.")
  #query("Flique(;x:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d);x=<<COUNT(*)>>.")




if __name__ == "__main__": main()
