import os
import wrappertemplate

def loadQuery(name):
    name = "queries." + name
    mod = __import__(name, fromlist=[''])
    return mod

def compileQuery(name):
	os.chdir("wrapper")
	os.system("./build.sh " + name + ">/dev/null 2>&1")
	os.chdir("..")

def execute(name):
	wrapperfile = open("wrapper/querywrapper.cpp","w")
	wrapperfile.write(wrappertemplate.getCode(name))
	wrapperfile.close()

	compileQuery(name)
	q = loadQuery(name)
	q_result = q.run()
	print q.num_rows(q_result)