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
  command = QUERY_COMPILER_RUN_SCRIPT+" -c "+environment.config["database"]+"/config.json \""+ re.escape(datalog_string) + "\""
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
    fetchedData = codegenerator.fetchRelation.fetch(relation,environment)
    return pd.DataFrame.from_records(data=fetchedData)

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
  #db_config="/afs/cs.stanford.edu/u/caberger/config.json"
  #db_config="/Users/caberger/Documents/Research/data/databases/higgs/config.json"

  db_config="/Users/caberger/Documents/Research/data/lubm1000/config.json"
  #createDB(db_config)
  loadDB("/Users/caberger/Documents/Research/data/lubm1000/db")
  
  #loadDB("$EMPTYHEADED_HOME/examples/graph/data/simple/db")

  #query("Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).")
  #a=fetchData("Triangle")
  #query("Triangle(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c);m=<<COUNT(*)>>.")
  #query("Flique(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d);m=<<COUNT(*)>>.")

  #query("Lollipop(a,b,c,x) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x).")
  #a= fetchData("Lollipop")
  #print a

  #query("Barbell(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z).")
  #a= fetchData("Barbell")
  #print a

if __name__ == "__main__": main()
