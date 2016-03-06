import numpy as np
import pandas as pd
from emptyheaded import *

class ResultError(Exception):
    pass

def lubm1(db):
  lubm1 = \
"""
lubm1(a) :- b='http://www.Department0.University0.edu/GraduateCourse0',
  c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent',
  takesCourse(a,b),rdftype(a,c).
"""
  print "\nLUBM 1"
  db.eval(lubm1)
  tri = db.get("lubm1")

  if tri.num_rows != 4:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

def lubm2(db):
  lubm2 = \
"""
lubm2(a,b,c) :- x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent',
  y='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department',
  z='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University',
  memberOf(a,b),subOrganizationOf(b,c),undegraduateDegreeFrom(a,c),rdftype(a,x),rdftype(b,y),rdftype(c,z).
"""
  print "\nLUBM 2"
  db.eval(lubm2)
  tri = db.get("lubm2")

  if tri.num_rows != 0:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

def lubm4(db):
  lubm4 = \
"""
lubm4(a,b,c,d) :- e='http://www.Department0.University0.edu',
  f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor',
  worksFor(a,e),name(a,b),emailAddress(a,d),telephone(a,c),rdftype(a,f).
"""
  print "\nLUBM 4"
  db.eval(lubm4)
  tri = db.get("lubm4")

  if tri.num_rows != 14:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

def lubm6(db):
  lubm6 = \
"""
lubm6(a) :- b='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent',
  rdftype(a,b).
"""
  print "\nLUBM 6"
  db.eval(lubm6)
  tri = db.get("lubm6")

  if tri.num_rows != 5916:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

def lubm7(db):
  lubm7 = \
"""
lubm7(a,b) :- c='http://www.Department0.University0.edu/AssociateProfessor0',
  d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course',
  e='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent',
  teacherOf(c,b),takesCourse(a,b),rdftype(b,d),rdftype(a,e).
"""
  print "\nLUBM 7"
  db.eval(lubm7)
  tri = db.get("lubm7")

  if tri.num_rows != 59:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

def lubm8(db):
  lubm8 = \
"""
lubm8(a,b,c) :- d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent',
  e='http://www.University0.edu',
  f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department',
  memberOf(a,b),emailAddress(a,c),rdftype(a,d),subOrganizationOf(b,e),rdftype(b,f).
"""
  print "\nLUBM 8"
  db.eval(lubm8)
  tri = db.get("lubm8")

  if tri.num_rows != 5916:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))

def lubm12(db):
  lubm12 = \
"""
lubm12(a,b) :- c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor',
  d='http://www.University0.edu',
  e='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department',
  worksFor(a,b),rdftype(a,c),subOrganizationOf(b,d),rdftype(b,e).
"""
  print "\nLUBM 12"
  db.eval(lubm12)
  tri = db.get("lubm12")

  if tri.num_rows != 125:
    raise ResultError("NUMBER OF ROWS INCORRECT: " + str(tri.num_rows))


def test_lubm():
  build = True

  takesCourse = Relation(
    name="takesCourse",
    schema=Schema(attributes=["string","string"]),
    filename=os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/data/takesCourse.tsv")

  memberOf = Relation(
    name="memberOf",
    schema=Schema(attributes=["string","string"]),
    filename=os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/data/memberOf.tsv")

  subOrganizationOf = Relation(
    name="subOrganizationOf",
    schema=Schema(attributes=["string","string"]),
    filename=os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/data/subOrganizationOf.tsv")

  undegraduateDegreeFrom = Relation(
    name="undegraduateDegreeFrom",
    schema=Schema(attributes=["string","string"]),
    filename=os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/data/undergraduateDegreeFrom.tsv")

  rdftype = Relation(
    name="rdftype",
    schema=Schema(attributes=["string","string"]),
    filename=os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/data/rdftype.tsv")

  worksFor = Relation(
    name="worksFor",
    schema=Schema(attributes=["string","string"]),
    filename=os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/data/worksFor.tsv")

  name = Relation(
    name="name",
    schema=Schema(attributes=["string","string"]),
    filename=os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/data/name.tsv")

  emailAddress = Relation(
    name="emailAddress",
    schema=Schema(attributes=["string","string"]),
    filename=os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/data/emailAddress.tsv")

  telephone = Relation(
    name="telephone",
    schema=Schema(attributes=["string","string"]),
    filename=os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/data/telephone.tsv")

  teacherOf = Relation(
    name="teacherOf",
    schema=Schema(attributes=["string","string"]),
    filename=os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/data/teacherOf.tsv")

  if build:
    db = Database.create(
      Config(num_threads=4),
      os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/databases/db_lubm1",
      [ takesCourse,
        memberOf,
        subOrganizationOf,
        undegraduateDegreeFrom,
        rdftype,
        worksFor,
        name,
        emailAddress,
        telephone,
        teacherOf ] )
    db.build()
  db = Database.from_existing(os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/databases/db_lubm1")

  lubm1(db)
  lubm2(db)
  lubm4(db)
  lubm6(db)
  lubm7(db)
  lubm8(db)
  lubm12(db)

#basically the main method down here.
start()
os.system("rm -rf "+os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/databases"+" && mkdir -p "os.path.expandvars("$EMPTYHEADED_HOME")+"/test/rdf/databases")
test_lubm()
stop()