import os
import code.wrappertemplate
import imp

def loadQuery(name):
	imp.acquire_lock()
	fname = os.path.expandvars("$EMPTYHEADED_HOME/runtime/queries/") + name +".so"
	mod = imp.load_dynamic(name,fname)
	imp.release_lock()
	return mod

def compileQuery(name):
	mydir=os.getcwd()
	os.chdir(os.path.expandvars("$EMPTYHEADED_HOME/runtime/wrapper"))
	os.system("./build.sh " + name + ">/dev/null")
	os.chdir(mydir)

def execute(name,mem,types,annotationType):
	wrapperfile = open(os.path.expandvars("$EMPTYHEADED_HOME/runtime/wrapper/querywrapper.cpp"),"w")
	wrapperfile.write(code.wrappertemplate.getCode(name,mem,types,annotationType))
	wrapperfile.close()

	compileQuery(name)

	q = loadQuery(name)
	q_result = q.run()
	return (q,q_result)

def compile(name,mem,types,annotationType):
	wrapperfile = open(os.path.expandvars("$EMPTYHEADED_HOME/runtime/wrapper/querywrapper.cpp"),"w")
	wrapperfile.write(code.wrappertemplate.getCode(name,mem,types,annotationType))
	wrapperfile.close()
	compileQuery(name)

def run(name,mem,types,annotationType):
	q = loadQuery(name)
	q_result = q.run()
	return (q,q_result)
