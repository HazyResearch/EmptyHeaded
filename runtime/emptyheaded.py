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
import sys

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

##methods for regression testing
def pruned_graph(dataset,create):
  print "DATASET: " + dataset
  if create:
    db_config="/afs/cs.stanford.edu/u/caberger/config_pruned.json"
    os.system("sed -e 's/$DATASET/"+dataset+"/g' "+db_config+" > tmp.json")
    createDB("tmp.json")
  loadDB("/dfs/scratch0/caberger/datasets/"+dataset+"/db_python_pruned")
  print "RUNNING QUERY: COUNT_Triangle"
  query("CTriangle(;x:long) :- Edge(a,b),Edge(b,c),Edge(a,c);x=<<COUNT(*)>>.")
  print "RUNNING QUERY: Triangle"
  query("Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).")
  print "RUNNING QUERY: COUNT_Flique"
  query("CFlique(;x:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d);x=<<COUNT(*)>>.")
  #print "RUNNING QUERY: Flique"
  #query("Flique(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d).")

def duplicated_graph(dataset,create):
  print "DATASET: " + dataset
  if create:
    db_config="/afs/cs.stanford.edu/u/caberger/config.json"
    os.system("sed -e 's/$DATASET/"+dataset+"/g' "+db_config+" > tmp.json")
    createDB("tmp.json")
  loadDB("/dfs/scratch0/caberger/datasets/"+dataset+"/db_python")
  print "RUNNING QUERY: COUNT_Lollipop"
  query("CLollipop(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x);m=<<COUNT(*)>>.")
  #print "RUNNING QUERY: Lollipop"
  #query("Lollipop(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x).")
  print "RUNNING QUERY: COUNT_Barbell"
  query("CBarbell(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z);m=<<COUNT(*)>>.")
  #print "RUNNING QUERY: Barbell"
  #query("Barbell(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z).")

def lubm(create):
  print "DATASET: LUBM10000"
  if create:
    db_config="/afs/cs.stanford.edu/u/caberger/rdf.json"
    createDB(db_config)
  loadDB("/dfs/scratch0/caberger/datasets/lubm10000/db_python")
  print "RUNNING QUERY: LUBM1"
  query("lubm1(a) :- takesCourse(a,b='http://www.Department0.University0.edu/GraduateCourse0'),type(a,c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent').")
  print "RUNNING QUERY: LUBM2"
  query("lubm2(a,b,c) :- memberOf(a,b),subOrganizationOf(b,c),undegraduateDegreeFrom(a,c),type(a,x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent'),type(b,y='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department'),type(c,z='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University').")
  print "RUNNING QUERY: LUBM4"
  query("lubm4(a,b,c,d) :- worksFor(a,e='http://www.Department0.University0.edu'),name(a,b),emailAddress(a,d),telephone(a,c),type(a,f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor').")
  print "RUNNING QUERY: LUBM6"
  query("lubm6(a) :- type(a,b='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent').")
  print "RUNNING QUERY: LUBM7"
  query("lubm7(a,b) :- teacherOf(c='http://www.Department0.University0.edu/AssociateProfessor0',b),takesCourse(a,b),type(b,d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course'),type(a,e='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent').")
  print "RUNNING QUERY: LUBM8"
  query("lubm8(a,b,c) :- memberOf(a,b),emailAddress(a,c),type(a,d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent'),subOrganizationOf(b,e='http://www.University0.edu'),type(b,f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department').")
  print "RUNNING QUERY: LUBM12"
  query("lubm12(a,b) :- worksFor(a,b),type(a,c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor'),subOrganizationOf(b,d='http://www.University0.edu'),type(b,e='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department').")

#mainly just used for regression testing
def main(argv):
  if argv[0] == "pruned":
    pruned_graph(argv[1],False)
  elif argv[0] == "duplicated":
    duplicated_graph(argv[1],False)
  elif argv[0] == "lubm":
    lubm(False)
  else:
    print "Running"

if __name__ == "__main__": main(sys.argv[1:])
