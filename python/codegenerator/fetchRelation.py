import sys
import env
import itertools
import cppgenerator
import querytemplate
import code.fetch

from pprint import pprint

def fetch(relation,env):
	notFound = "Relation " + relation + " does not exist in database."
	if relation in env.schemas:
		schema = env.schemas[relation]
		ordering = schema["orderings"][0]
		trieName = relation + "_" + "_".join(map(str,ordering))
		types = map(lambda x : str(x["type"]),schema["attributes"])
		if trieName in env.relations:
			if env.relations[trieName] == "disk":
				#get encodings
				result = cppgenerator.compileAndRun(lambda: 
					fetchData(relation,trieName,schema["annotation"],schema["attributes"],env),
					"fetchData_"+relation,env.config["memory"],types,schema["annotation"])
				print "NUM ROWS: " + str(result[0].num_rows(result[1]))
				result[0].fetch_data(result[1])
			else:
				print "Not yet supported. Must be on disk."
		else:
			print notFound
	else:
		print notFound

def fetchData(relation,trieName,annotationType,attributes,env):
	memType = env.config["memory"]
	include = """#include "utils/thread_pool.hpp"
	#include "Trie.hpp"
	#include "utils/Timer.hpp"
	#include "utils/%(memType)s.hpp"
	"""% locals()
	path = env.config["database"] + "/relations/" + relation + "/" + trieName
	runCode = code.fetch.loadRelation(path,trieName,annotationType,memType)
	
	print attributes
	s = set()
	for a in attributes:
		if a["encoding"] not in s:
			runCode += code.fetch.loadEncoding(env.config["database"],a["encoding"],a["type"])
			s.add(a["encoding"])
	encodings = map(lambda i:i["encoding"],attributes)
	runCode += code.fetch.setResult(trieName,encodings)
	return querytemplate.getCode(include,runCode)
	#cppgenerator.compileAndRun(
	#lambda: cppgenerator.loadRelations(relations,env),
	#libname)
	#print relation