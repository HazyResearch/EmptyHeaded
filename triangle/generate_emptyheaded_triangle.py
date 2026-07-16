#!/usr/bin/env python
"""Generate EmptyHeaded's C++ plan for an oriented triangle relation.

The input relation is expected to contain only oriented edges i < j.  Thus the
three-way join itself is sufficient to impose i < j < k.  Graph IDs enter as
uint64 values and are dictionary-encoded to EmptyHeaded's legacy dense uint32
trie keys; the external value type and internal key type are intentionally
different.
"""

import glob
import os
import re
import shutil
import sys

import jpype

from make_portable_emptyheaded_header import make_portable


ROOT = os.environ["EMPTYHEADED_HOME"]
if len(sys.argv) > 4:
    raise SystemExit(
        "usage: generate_emptyheaded_triangle.py [gNN] [threads] [--query-only]")
SCALE = sys.argv[1] if len(sys.argv) > 1 else "g12"
if not re.match(r"^g[0-9]+$", SCALE):
    raise ValueError("scale must look like g12 or g16")
NUM_THREADS = int(sys.argv[2]) if len(sys.argv) > 2 else 16
if NUM_THREADS < 1:
    raise ValueError("threads must be positive")
QUERY_ONLY = len(sys.argv) > 3 and sys.argv[3] == "--query-only"
if len(sys.argv) > 3 and not QUERY_ONLY:
    raise ValueError("the only supported third argument is --query-only")
ARTIFACT_NAME = SCALE if NUM_THREADS == 16 else "%s-t%d" % (SCALE, NUM_THREADS)
OUTPUT = os.path.join(ROOT, "triangle", "generated", ARTIFACT_NAME)
DATABASE = os.path.join(OUTPUT, "database")
QUERY_FOLDER = "triangle_query"
QUERY = """
TriangleCount(;count) :- Edge(i,j),Edge(j,k),Edge(i,k),count:long<-[COUNT(*)].
"""


def main():
    if QUERY_ONLY:
        if not os.path.isdir(DATABASE):
            raise RuntimeError("existing database required for --query-only: " + DATABASE)
    else:
        if os.path.isdir(OUTPUT):
            shutil.rmtree(OUTPUT)
        os.makedirs(OUTPUT)

    jars = ":".join(glob.glob(os.path.join(
        ROOT, "query_compiler", "target", "pack", "lib", "*.jar")))
    jpype.startJVM(jpype.getDefaultJVMPath(), "-Djava.class.path=" + jars)
    try:
        duncecap = jpype.JPackage("duncecap")
        strings = jpype.JArray(jpype.JString)
        schema = duncecap.QueryCompiler.buildSchema(
            strings(["uint64", "uint64"]),
            strings([]),
            strings(["i", "j"]),
            strings([]))
        relation = duncecap.Relation(
            "Edge", schema, "/input/edges.tsv", False)
        config = duncecap.Config(
            "emptyheaded", NUM_THREADS, 1, "hybrid", "RAM")
        db = duncecap.DBInstance(DATABASE, config)
        db.addRelation(relation)
        compiler = duncecap.QueryCompiler(db, "triangle")
        if not QUERY_ONLY:
            compiler.toDisk()
            compiler.createDB()
        else:
            # QueryPlan.generate copies a template directory and expects the
            # destination not to exist. Remove only the generated query
            # wrapper; persisted encodings and relation tries stay untouched.
            old_query_dir = os.path.join(DATABASE, "libs", QUERY_FOLDER)
            if os.path.isdir(old_query_dir):
                shutil.rmtree(old_query_dir)
        generated_units = compiler.generate(
            QUERY, "triangle", QUERY_FOLDER, False)
        if generated_units != 1:
            raise RuntimeError("expected one generated unit, got %s" % generated_units)
    finally:
        jpype.shutdownJVM()

    query_dir = os.path.join(DATABASE, "libs", QUERY_FOLDER)
    generated_header = os.path.join(OUTPUT, "run_0.generated.hpp")
    portable_header = os.path.join(OUTPUT, "run_0.hpp")
    shutil.copy2(os.path.join(query_dir, "run_0.hpp"), generated_header)
    make_portable(generated_header, portable_header, DATABASE)
    with open(os.path.join(OUTPUT, "triangle_count.dl"), "w") as stream:
        stream.write(QUERY.strip() + "\n")
    print(portable_header)


if __name__ == "__main__":
    main()
