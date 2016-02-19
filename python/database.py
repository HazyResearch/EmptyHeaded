## High-level class to store the database
## The database contains a filename and relations
## Relations contain schemas
## Database -> Relations -> Schema

## Only one database should be created per python process.
## Database spins up the JVM which serves as our Query Compiler.

import jpype
from config import Config
from parsers import *
from relation import Relation
from DB import DB
import os

dbhash = 0

class Database:
  #pulls the data from EH into a pandas dataframe
  def get(self,name):
    #code generation
    self.qc.genTrieWrapper(name)
    #execution
    return self.backend.get(name)

  def load(self,name):
    #code generation
    self.qc.genTrieWrapper(name)
    #execution
    self.backend.load(name)

  #parses, codegens, and runs
  def sql(self,sql):
    self.qc.sql(sql)
    execute("Query")

  #parses, codegens, and runs
  def datalog(self,datalog):
    self.qc.datalog(datalog)
    execute("Query")

  #runs GHD optimizer
  #returns an IR
  def optimize(self,ir):
    jir = ir.python2java()
    self.qc.optimize(jir)

  #codegen and execute a query
  def eval(self,ir):
    #code generation
    #FIXME
    #execution
    self.backend.query("query")
    print "Return a dataframe from the DB"

  #should return a (Trie,name,ordering)
  def execute(filename):
    print "compiles and executes query"

  def compile_backend(self):
    #compilation (compiles EH backend and the creation query)
    storage_engine = os.environ['EMPTYHEADED_HOME']+"/storage_engine"
    #clean up the previous build
    os.system("""rm -rf """+storage_engine+"""/build && mkdir """+storage_engine+"""/build""")
    #make the backend
    os.system("""cd """+storage_engine+"/build && cmake -DNUM_THREADS="+str(self.config.num_threads)+" .. && make && cd - > /dev/null" )

  #Build the db in backend and save to disk
  def build(self):
    #Save our schemas to disk (so we can run from_existing)
    self.qc.toDisk()
    #code generation
    self.qc.createDB()
    #compile the generated load query
    self.compile_backend()
    os.system("""cd """+self.folder+"""/libs/createDB && ./build.sh && cd - > /dev/null""")
    #execution
    self.backend.create(self.relations,self.dbhash)

  #reads files from an existing database on disk
  #takes java QC and translates it to respective python classes 
  @staticmethod
  def from_existing(folder):
    self = Database()
    self.duncecap = jpype.JPackage('duncecap')
    self.folder = folder #string
    self.qc = self.duncecap.QueryCompiler.fromDisk(self.folder+"/schema.bin")
    
    dbInstance = self.qc.getDBInstance()
    self.folder = dbInstance.getFolder()
    self.config = Config.java2python(dbInstance.getConfig())

    num_relations = dbInstance.getNumRelations()
    self.relations = []
    for i in range(0,num_relations):
      self.relations.append(
        Relation.java2python(dbInstance.getRelation(i)))
    self.backend = DB(self.folder)
    self.dbhash = str(dbhash)

    return self

  #create a database from scratch
  @staticmethod
  def create(config,folder,relations):
    global dbhash
    self = Database()
    self.folder = folder #string
    self.relations = relations #list of Relation (relation.py)
    self.duncecap = jpype.JPackage('duncecap')
    self.config = config # Config.py

    javaConfig = self.duncecap.Config(
      config.system,
      config.num_threads,
      config.num_sockets,
      config.layout,
      config.memory)
    javaDB = self.duncecap.DBInstance(folder,javaConfig)
    for relation in relations:
      javaDB.addRelation(relation.python2java(self.duncecap))
    self.dbhash = str(dbhash)
    self.qc = self.duncecap.QueryCompiler(javaDB,str(dbhash))
    dbhash += 1

    #execution
    self.backend = DB(self.folder)
    return self

  #for printing purposes
  def __repr__(self):
    return """Database< liverelations:%s \n\t  folder:%s
\t  Config%s
\t  Relations%s >""" % (self.liverelations,self.folder,self.config,self.relations)
