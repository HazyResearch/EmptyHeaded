import sys
from schema import Schema 
from relation import Relation 
from database import Database 
from config import Config
from parsers import *
from ir import *
import glob
import jpype
import os

#launch the JVM
def start():
  ehhome = os.path.expandvars("$EMPTYHEADED_HOME")
  jars = (":").join(glob.glob(ehhome+"/query_compiler/target/pack/lib/*.jar"))
  jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path="+jars)

#kill the JVM
def stop():
  jpype.shutdownJVM()

def main(argv):
  start()
  graph = Relation(
    name="graph",
    schema=Schema(["Integer","Integer"]),
    file="graph.tsv")

  db = Database.create(
    Config(),
    "db",
    [graph])
  db.build()
  print db
  db = Database.from_existing("db")

  s_ir = SQL("SELECT a FROM R")
  d_ir = Datalog("Triangle :- R(a,b),R(b,c),R(a,c).")
  print db

  rule = RULE(
    RESULT(RELATION(name="Triangle",attributes=["a","b"])),
    ORDER(attributes=["a","b","c"]),
    PROJECT(attributes=["c"]),
    OPERATION(operation="*"),
    JOIN([
      RELATION(name="R",attributes=["a","b"],scaling=1.0),
      RELATION(name="R",attributes=["b","c"]),
      RELATION(name="R",attributes=["a","c"])]),
    AGGREGATES([
      AGGREGATE(operation="SUM",attributes=["c"])]),
    FILTERS([
      SELECT(attribute="a",operation="=",value="DOG"),
      SELECT(attribute="b",operation="=",value="10")])
  )
  print rule

  ir = IR([rule])

  jir = ir.python2java()
  pir = IR.java2python(jir)
  print pir
  print pir.rules[0]

  stop()

if __name__ == "__main__": main(sys.argv[1:])