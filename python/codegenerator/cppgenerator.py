import cppexecutor
import os
import querytemplate
import code.build
from sets import Set

def generate(f,name):
	#generate C++ code
	codeString = f()
	os.system("mkdir -p ../storage_engine/generated")
	cppfile = open("../storage_engine/generated/"+name+".cpp","w")
	cppfile.write(codeString)
	cppfile.close()
	os.system("clang-format -style=llvm -i ../storage_engine/generated/"+name+".cpp")

def compileC(name):
	#compile C++ code
	os.chdir("../storage_engine")
	try:
		os.system("make "+name+" >/dev/null")
	except:
		print "C++ compilation failed"
		sys.exit(1)

	os.chdir("../python")

def compileAndRun(f,name):
	generate(f,name)
	compileC(name)
	#generates python wrapper and excutes C++ code
	cppexecutor.execute(name)

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

	for encoding in encodings:
		name,types = encoding
		os.system("mkdir -p " + env.config["database"] + "/encodings/"+name)
		codeString += code.build.buildAndDumpEncoding(env.config["database"],encoding)
	
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
	return querytemplate.getCode(include,runCode)

def buildTrie(orderings,relation,env):
	include = """#include "emptyheaded.hpp" """
	codeString = code.build.loadEncodedRelation(env.config["database"],relation["name"])
	for ordering in orderings:
		roname = relation["name"] + "_" + "_".join(map(str,ordering))
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
	return querytemplate.getCode(include,codeString)

def nprr():
	print "nprr"

def top_down():
	print "top down"
