## Contains the bridge for each front-end parser. 
## The parser pontentially spins up the JVM 
## creates the respective object in scala
## Sends the string to the object which returns an IR

import jpype
from ir import * 

class Parser:
  def __init__(self):
    self.duncecap = jpype.JPackage('duncecap')

class sql(Parser):
  def __init__(self,query):
    Parser.__init__(self)
    ir = self.duncecap.SQL(query).parse()
    #return IR.java2python(ir)

class datalog(Parser):
  def __init__(self,query):
    Parser.__init__(self)
    self.jir = self.duncecap.Datalog(query).parse()
    self.ir = IR.java2python(self.jir)