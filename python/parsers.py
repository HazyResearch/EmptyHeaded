## Contains the bridge for each front-end parser. 
## The parser pontentially spins up the JVM 
## creates the respective object in scala
## Sends the string to the object which returns an IR

import jpype

class Parser:
  def __init__(self):
    self.duncecap = jpype.JPackage('duncecap')

class SQL(Parser):
  def __init__(self,query):
    Parser.__init__(self)
    ir = self.duncecap.SQL(query).parse()
    #return IR.java2python(ir)

class Datalog(Parser):
  def __init__(self,query):
    Parser.__init__(self)
    ir = self.duncecap.SQL(query).parse()
    #return IR.java2python(ir)