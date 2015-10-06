import sys
import json
import env
import itertools
import cppgenerator

from pprint import pprint

def printraw(s):
	sys.stdout.write(s)

def generateAllOrderings(num):
	return list(itertools.permutations(range(0,num)))

def printLoadedRelation(relation):
	printraw("\t" + relation["name"] + "(")
	printRels = map(lambda x: str(x["encoding"]+":"+x["type"]),relation["attributes"])
	printraw(",".join(printRels))
	print ")"

def fromJSON(path,env):
	data = json.load(open(path))
	relations = data.pop("relations",0)
	env.setup(data)
	for relation in relations:
		libname = "load_"+relation["name"]
		cppgenerator.compileAndRun(lambda: cppgenerator.loadRelation(relation,env),libname)

	for relation in relations:
		attributes = relation["attributes"]
		#grab the orderings (create them if all is specified)
		orderings = relation["orderings"]
		if len(orderings) == 1 and orderings[0] == "all":
			orderings = generateAllOrderings(len(attributes))
		for ordering in orderings:
			cppgenerator.buildTrie(ordering,relation,env)

	print "Created database with the following relations: "
	for relation in relations:
		printLoadedRelation(relation)
