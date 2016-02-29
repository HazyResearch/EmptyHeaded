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
import numpy as np
import pandas as pd

#launch the JVM
def start():
  ehhome = os.path.expandvars("$EMPTYHEADED_HOME")
  jars = (":").join(glob.glob(ehhome+"/query_compiler/target/pack/lib/*.jar"))
  jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path="+jars)

#kill the JVM
def stop():
  jpype.shutdownJVM()

if __name__ == "__main__": main(sys.argv[1:])