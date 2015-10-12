import os
import wrappertemplate

def loadQuery(name):
    name = "queries." + name
    mod = __import__(name, fromlist=[''])
    return mod

def compileQuery(name):
	os.chdir("wrapper")
	#print name
	os.system("./build.sh " + name + ">/dev/null")
	os.chdir("..")

def execute(name,mem,types,annotationType):
	wrapperfile = open("wrapper/querywrapper.cpp","w")
	wrapperfile.write(wrappertemplate.getCode(name,mem,types,annotationType))
	wrapperfile.close()

	compileQuery(name)
	q = loadQuery(name)
	q_result = q.run()
	return (q,q_result)