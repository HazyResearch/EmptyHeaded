import cppexecutor
import os

def generate(f,name):
	#generate C++ code
	codeString = f()
	os.system("mkdir -p "+os.environ["EMPTYHEADED_HOME"]+"/storage_engine/generated")
	cppfile = open(os.environ["EMPTYHEADED_HOME"]+"/storage_engine/generated/"+name+".cpp","w")
	cppfile.write(codeString)
	cppfile.close()
	os.system("clang-format -style=llvm -i "+os.environ["EMPTYHEADED_HOME"]+"/storage_engine/generated/"+name+".cpp")

def compileC(name):
	#compile C++ code
	os.chdir(os.environ["EMPTYHEADED_HOME"]+"/storage_engine")
	try:
		os.system("make "+name+" >/dev/null")
	except:
		print "C++ compilation failed"
		sys.exit(1)

	os.chdir(os.environ["EMPTYHEADED_HOME"]+"/runtime")

def compileAndRun(f,name,mem,types,annotationType):
	#os.system("rm -rf ../libs/lib" + name + ".so")
	generate(f,name) #generates C++ code
	compileC(name) #compiles C++ library
	return cppexecutor.execute(name,mem,types,annotationType) 	#generates python wrapper and excutes C++ code

def compile(f,name,mem,types,annotationType):
	generate(f,name) #generates C++ code
	compileC(name) #compiles C++ library