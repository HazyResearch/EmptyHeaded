import sys
import json
import env
import itertools
import cppgenerator
import code.querytemplate
import os
from sets import Set
import code.build

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
	
	libname = "loadDB"
	a = cppgenerator.compileAndRun(
		lambda: loadRelations(relations,env),
		libname,env.config["memory"],[],"void*")

	envRelations = {}
	for relation in relations:
		attributes = relation["attributes"]
		#grab the orderings (create them if all is specified)
		orderings = relation["orderings"]
		if len(orderings) == 1 and orderings[0] == "all":
			orderings = generateAllOrderings(len(attributes))
		r = cppgenerator.compileAndRun(lambda: buildTrie(orderings,relation,env),
			"build_"+relation["name"],env.config["memory"],[],"void*")

		envRelations[relation["name"]] = { \
			"orderings":orderings,\
			"annotation":relation["annotation"],\
			"attributes":relation["attributes"]}
	env.setSchemas(envRelations)

	env.toJSON(env.config["database"]+"/config.json")
	print "Created database with the following relations: "
	for relation in relations:
		printLoadedRelation(relation)


##########
#Code generation methods
def loadRelationCode(relations,env):
	encodings = Set()
	codeString = ""
	os.system("mkdir -p " + env.config["database"])
	os.system("mkdir -p " + env.config["database"] + "/encodings")
	os.system("mkdir -p " + env.config["database"] + "/relations")

	for relation in relations:
		os.system("mkdir -p " + env.config["database"] + "/relations/"+relation["name"])

		types = ",".join(list(map(lambda x: str(x["type"]),relation["attributes"])))
		relencodings = list(map(lambda x: (str(x["encoding"]),str(x["type"])),relation["attributes"]))

		codeString += code.build.declareColumnStore(relation["name"],types)
		codeString += code.build.declareAnnotationStore(relation["annotation"])
		for e in set(relencodings):
			encodings.add(e)
			codeString += code.build.declareEncoding(e)
		codeString += code.build.readRelationFromTSV(relation["name"],relencodings,relation["source"])

	envEncodings = {}
	for encoding in encodings:
		name,types = encoding
		os.system("mkdir -p " + env.config["database"] + "/encodings/"+name)
		codeString += code.build.buildAndDumpEncoding(env.config["database"],encoding)
		envEncodings[name] = types
	env.setEncodings(envEncodings)

	for relation in relations:
		relencodings = list(map(lambda x: (str(x["encoding"]),str(x["type"])),relation["attributes"]))
		codeString += code.build.encodeRelation(
			env.config["database"],
			relation["name"],
			relencodings,
			relation["annotation"])
	return codeString

def loadRelations(relations,env):
	include = """#include "emptyheaded.hpp" """
	runCode = loadRelationCode(relations,env)
	return code.querytemplate.getCode(include,runCode)

def buildTrie(orderings,relation,env):
	include = """#include "emptyheaded.hpp" """
	codeString = code.build.loadEncodedRelation(env.config["database"],relation["name"])
	
	envRelations = {}
	for ordering in orderings:
		roname = relation["name"] + "_" + "_".join(map(str,ordering))
		envRelations[roname] = "disk"
		trieFolder = env.config["database"] + "/relations/"+relation["name"]+"/"+roname
		os.system("mkdir -p " + trieFolder)
		os.system("mkdir -p " + trieFolder +"/mmap")
		os.system("mkdir -p " + trieFolder+"/ram")

		codeString += code.build.buildOrder(
			trieFolder,
			relation["name"],
			ordering,
			relation["annotation"],
			env.config["memory"])
	env.setRelations(envRelations)
	return code.querytemplate.getCode(include,codeString)
