import cppexecutor
import os
import querytemplate
from sets import Set
import codeCreateDB

def generate(f,name):
	#generate C++ code
	code = f()
	os.system("mkdir -p ../storage_engine/generated")
	cppfile = open("../storage_engine/generated/"+name+".cpp","w")
	cppfile.write(code)
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
	code = ""
	os.system("mkdir -p " + env.config["database"])
	os.system("mkdir -p " + env.config["database"] + "/encodings")
	os.system("mkdir -p " + env.config["database"] + "/relations")

	for relation in relations:
		os.system("mkdir -p " + env.config["database"] + "/relations/"+relation["name"])

		types = ",".join(list(map(lambda x: str(x["type"]),relation["attributes"])))
		relencodings = list(map(lambda x: (str(x["encoding"]),str(x["type"])),relation["attributes"]))

		code += codeCreateDB.declareColumnStore(relation["name"],types)
		code += codeCreateDB.declareAnnotationStore(relation["annotation"])
		for e in set(relencodings):
			encodings.add(e)
			code += codeCreateDB.declareEncoding(e)
		code += codeCreateDB.readRelationFromTSV(relation["name"],relencodings,relation["source"])

	for encoding in encodings:
		name,types = encoding
		os.system("mkdir -p " + env.config["database"] + "/encodings/"+name)
		code += codeCreateDB.buildAndDumpEncoding(env.config["database"],encoding)
	
	for relation in relations:
		relencodings = list(map(lambda x: (str(x["encoding"]),str(x["type"])),relation["attributes"]))
		code += codeCreateDB.encodeRelation(
			env.config["database"],
			relation["name"],
			relencodings,
			relation["annotation"])
	return code



def loadRelations(relations,env):
	include = """#include "emptyheaded.hpp" """
	runCode = loadRelationCode(relations,env)
	return querytemplate.getCode(include,runCode)

def buildTrie(orderings,relation,env):
	include = """#include "emptyheaded.hpp" """
	code = codeCreateDB.loadEncodedRelation(env.config["database"],relation["name"])
	for ordering in orderings:
		roname = relation["name"] + "_" + "_".join(map(str,ordering))
		trieFolder = env.config["database"] + "/relations/"+relation["name"]+"/"+roname
		os.system("mkdir -p " + trieFolder)
		os.system("mkdir -p " + trieFolder +"/mmap")
		os.system("mkdir -p " + trieFolder+"/ram")

		code += codeCreateDB.buildOrder(
			trieFolder,
			relation["name"],
			ordering,
			relation["annotation"],
			env.config["memory"])
	return querytemplate.getCode(include,code)

def nprr():
	print "nprr"

def top_down():
	print "top down"
