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

#basically the main method down here.
start()
test_lubm()
stop()