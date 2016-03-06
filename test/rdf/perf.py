import numpy as np
import pandas as pd
from emptyheaded import *

class ResultError(Exception):
    pass

def lubm1(db):
  lbm1 = \
"""
lubm1(a) :- b='http://www.Department0.University0.edu/GraduateCourse0',
  c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent',
  takesCourse(a,b),rdftype(a,c).
"""
  print "\nQUERY: LUBM 1"
  db.eval(lbm1)

def lubm2(db):
  lbm2 = \
"""
lubm2(a,b,c) :- x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent',
  y='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department',
  z='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University',
  memberOf(a,b),subOrganizationOf(b,c),undergraduateDegreeFrom(a,c),rdftype(a,x),rdftype(b,y),rdftype(c,z).
"""
  print "\nQUERY: LUBM 2"
  db.eval(lbm2)

def lubm3(db):
  lbm3 = \
"""
lubm3(a) :- b='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Publication',
  c='http://www.Department0.University0.edu/AssistantProfessor0',
  rdftype(a,b),publicationAuthor(a,c).
"""
  print "\nQUERY: LUBM 3"
  db.eval(lbm3)

def lubm4(db):
  lbm4 = \
"""
lubm4(a,b,c,d) :- e='http://www.Department0.University0.edu',
  f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssociateProfessor',
  worksFor(a,e),name(a,b),emailAddress(a,d),telephone(a,c),rdftype(a,f).
"""
  print "\nQUERY: LUBM 4"
  db.eval(lbm4)

def lubm5(db):
  lbm5 = \
"""
lubm5(a) :- b='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent',
  c='http://www.Department0.University0.edu',
  rdftype(a,b),memberOf(a,c).
"""
  print "\nQUERY: LUBM 5"
  db.eval(lbm5)

def lubm7(db):
  lbm7 = \
"""
lubm7(b,c) :- a='http://www.Department0.University0.edu/AssociateProfessor0',
  d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course',
  e='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent',
  teacherOf(a,b),takesCourse(c,b),rdftype(b,d),rdftype(c,e).
"""
  print "\nQUERY: LUBM 7"
  db.eval(lbm7)

def lubm8(db):
  lbm8 = \
"""
lubm8(a,b,c) :- d='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent',
  e='http://www.University0.edu',
  f='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department',
  memberOf(a,b),emailAddress(a,c),rdftype(a,d),subOrganizationOf(b,e),rdftype(b,f).
"""
  print "\nQUERY: LUBM 8"
  db.eval(lbm8)

def lubm9(db):
  lbm9 = \
"""
lubm9(a,b,c) :- x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent',
  y='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course',
  z='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#AssistantProfessor',
  rdftype(a,x),rdftype(b,y),rdftype(c,z),advisor(a,c),teacherOf(c,b),takesCourse(a,b).
"""
  print "\nQUERY: LUBM 9"
  db.eval(lbm9)


def lubm11(db):
  lbm11 = \
"""
lubm11(a) :- x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#ResearchGroup',
  y='http://www.University0.edu',
  rdftype(a,x),subOrganizationOf(a,y).
"""
  print "\nQUERY: LUBM 11"
  db.eval(lbm11)

def lubm12(db):
  lbm12 = \
"""
lubm12(a,b) :- c='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor',
  d='http://www.University0.edu',
  e='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department',
  worksFor(b,a),rdftype(b,c),subOrganizationOf(a,d),rdftype(a,e).
"""
  print "\nQUERY: LUBM 12"
  db.eval(lbm12)

def lubm13(db):
  lbm13 = \
"""
lubm13(a) :- x='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent',
  y='http://www.University567.edu',
  rdftype(a,x),undergraduateDegreeFrom(a,y).
"""
  print "\nQUERY: LUBM 13"
  db.eval(lbm13)

def lubm14(db):
  lbm14 = \
"""
lubm14(a) :- b='http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#UndergraduateStudent',
  rdftype(a,b).
"""
  print "\nQUERY: LUBM 14"
  db.eval(lbm14)

def test_lubm():
  build = False

  takesCourse = Relation(
    name="takesCourse",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/takesCourse.tsv")

  memberOf = Relation(
    name="memberOf",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/memberOf.tsv")

  advisor = Relation(
    name="advisor",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/advisor.tsv")

  publicationAuthor = Relation(
    name="publicationAuthor",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/publicationAuthor.tsv")

  subOrganizationOf = Relation(
    name="subOrganizationOf",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/subOrganizationOf.tsv")

  undergraduateDegreeFrom = Relation(
    name="undergraduateDegreeFrom",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/undergraduateDegreeFrom.tsv")

  rdftype = Relation(
    name="rdftype",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/rdftype.tsv")

  worksFor = Relation(
    name="worksFor",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/worksFor.tsv")

  name = Relation(
    name="name",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/name.tsv")

  emailAddress = Relation(
    name="emailAddress",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/emailAddress.tsv")

  telephone = Relation(
    name="telephone",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/telephone.tsv")

  teacherOf = Relation(
    name="teacherOf",
    schema=Schema(attributes=["string","string"]),
    filename="/dfs/scratch0/caberger/datasets/eh_datasets/lubm10000/teacherOf.tsv")

  if build:
    db = Database.create(
      Config(num_threads=56),
      "/dfs/scratch0/caberger/datasets/eh_datasets/databases/db_lubm",
      [ takesCourse,
        memberOf,
        subOrganizationOf,
        advisor,
        publicationAuthor,
        undergraduateDegreeFrom,
        rdftype,
        worksFor,
        name,
        emailAddress,
        telephone,
        teacherOf ] )
    db.build()
  db = Database.from_existing("/dfs/scratch0/caberger/datasets/eh_datasets/databases/db_lubm")

  lubm1(db)
  lubm2(db)
  lubm3(db)
  lubm4(db)
  lubm5(db)
  lubm7(db)
  lubm8(db)
  lubm9(db)
  lubm11(db)
  lubm12(db)
  lubm13(db)
  lubm14(db)

#basically the main method down here.
start()
test_lubm()
stop()