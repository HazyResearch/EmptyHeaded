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
  query("CTriangle(;x:long) :- Edge(a,b),Edge(b,c),Edge(a,c);x=[<<COUNT(*)>>].")
  print "RUNNING QUERY: Triangle"
  query("Triangle(a,b,c) :- Edge(a,b),Edge(b,c),Edge(a,c).")
  print "RUNNING QUERY: COUNT_Flique"
  query("CFlique(;x:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d);x=[<<COUNT(*)>>].")
  #print "RUNNING QUERY: Flique"
  #query("Flique(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,d),Edge(b,d),Edge(c,d).")

def duplicated_graph(dataset,startNode,create):
  print "START NODE: " + startNode
  print "DATASET: " + dataset
  if create:
    db_config="/afs/cs.stanford.edu/u/caberger/config.json"
    os.system("sed -e 's/$DATASET/"+dataset+"/g' "+db_config+" > tmp.json")
    createDB("tmp.json")
  loadDB("/dfs/scratch0/caberger/datasets/"+dataset+"/db_python")
  print "RUNNING QUERY: COUNT_Lollipop"
  query("CLollipop(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x);m=[<<COUNT(*)>>].")
  #print "RUNNING QUERY: Lollipop"
  #query("Lollipop(a,b,c,d) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x).")
  print "RUNNING QUERY: COUNT_Barbell"
  query("CBarbell(;m:long) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z);m=[<<COUNT(*)>>].")
  #print "RUNNING QUERY: Barbell"
  #query("Barbell(a,b,c,x,y,z) :- Edge(a,b),Edge(b,c),Edge(a,c),Edge(a,x),Edge(x,y),Edge(y,z),Edge(x,z).")
  print "RUNNING QUERY: PageRank"
  query(
  """N(;w:int):-Edge(x,y);w=[<<COUNT(x)>>].
     PageRank(x;y:float):-Edge(x,z);y=[(1.0/N)*<<CONST(z;1.0)>>].
     PageRank (x;y:float)*[i=5]:-Edge(x,z),PageRank(z),InvDegree(z);y=[0.15+0.85*<<SUM(z;1.0)>>].""")
  print "RUNNING QUERY: SSSP"
  
  query("""SSSP(x;y:int) :- Edge(w=%(startNode)s,x);y=[<<CONST(w;1)>>].
    SSSP(x;y:int)*[c=0] :- Edge(w,x),SSSP(w);y=[1+<<MIN(w;1)>>]."""% locals())

def lubm(create):
  print "DATASET: LUBM10000"
  if create:
    db_config="/afs/cs.stanford.edu/u/caberger/rdf.json"
    createDB(db_config)
  loadDB("/dfs/scratch0/caberger/datasets/lubm10000/db_python")
  comm="""
  print "RUNNING QUERY: LUBM1"
  query("lubm1(a) :- takesCourse(a,b='http://www.Department0.University0.edu/GraduateCourse0'),rdftype(a,c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent').")
  print "RUNNING QUERY: LUBM2"
  query("lubm2(a,b,c) :- memberOf(a,b),subOrganizationOf(b,c),undergraduateDegreeFrom(a,c),rdftype(a,x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent'),rdftype(b,y='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department'),rdftype(c,z='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University').")
  print "RUNNING QUERY: LUBM3"
  query("lubm3(a) :- rdftype(a,b='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication'),publicationAuthor(a,c='http://www.Department0.University0.edu/AssistantProfessor0').")
  print "RUNNING QUERY: LUBM4"
  query("lubm4(a,b,c,d) :- worksFor(a,e='http://www.Department0.University0.edu'),name(a,b),emailAddress(a,d),telephone(a,c),rdftype(a,f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor').")
  print "RUNNING QUERY LUBM5"
  query("lubm5(a) :- rdftype(a,b='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent'),memberOf(a,c='http://www.Department0.University0.edu').")
  print "RUNNING QUERY: LUBM7"
  query("lubm7(b,c) :- teacherOf(a='http://www.Department0.University0.edu/AssociateProfessor0',b),takesCourse(c,b),rdftype(b,d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course'),rdftype(c,e='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent').")
  print "RUNNING QUERY: LUBM8"
  query("lubm8(a,b,c) :- memberOf(a,b),emailAddress(a,c),rdftype(a,d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent'),subOrganizationOf(b,e='http://www.University0.edu'),rdftype(b,f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department').")
  print "RUNNING QUERY: LUBM9"
  query("lubm9(a,b,c) :- rdftype(a,x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent'),rdftype(b,y='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course'),rdftype(c,z='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor'),advisor(a,c),teacherOf(c,b),takesCourse(a,b).")
  print "RUNNING QUERY: LUBM11"
  query("lubm11(a) :- rdftype(a,x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#ResearchGroup'),subOrganizationOf(a,y='http://www.University0.edu').")
  """
  print "RUNNING QUERY: LUBM12"
  query("lubm12(a,b) :- worksFor(b,a),rdftype(b,c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor'),subOrganizationOf(a,d='http://www.University0.edu'),rdftype(a,e='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department').")
  print "RUNNING QUERY: LUBM 13"
  query("lubm13(a) :- rdftype(a,x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent'),undergraduateDegreeFrom(a,y='http://www.University567.edu').")
  print "RUNNING QUERY: LUBM 14"
  query("lubm14(a) :- rdftype(a,b='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent').")

#mainly just used for regression testing
def main(argv):
  snodes={
    "cid-patents":"5795784",
    "socLivejournal":"10009",
    "higgs":"83222",
    "g_plus":"6966",
    "orkut":"43608",
    "twitter2010":"1037948"
  }

  if argv[0] == "pruned":
    pruned_graph(argv[1],True)
  elif argv[0] == "duplicated":
    duplicated_graph(argv[1],snodes[argv[1]],True)
  elif argv[0] == "lubm":
    lubm(False)
  else:
    print "Running"

if __name__ == "__main__": main(sys.argv[1:])
