import os
import code.wrappertemplate
import imp

def loadQuery(name):
	fname = "queries/" + name +".so"
	mod = imp.load_dynamic(name,fname)
	return mod

def compileQuery(name):
	os.chdir("wrapper")
	#print name
	os.system("./build.sh " + name + ">/dev/null")
	os.chdir("..")

def execute(name,mem,types,annotationType):
	wrapperfile = open("wrapper/querywrapper.cpp","w")
	wrapperfile.write(code.wrappertemplate.getCode(name,mem,types,annotationType))
	wrapperfile.close()

	compileQuery(name)
	q = loadQuery(name)
	q_result = q.run()
	return (q,q_result)