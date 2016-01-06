import cppexecutor
import os

def generate(f,hashstring):
	#generate C++ code
  codeString = f()
  os.system("mkdir -p "+os.environ["EMPTYHEADED_HOME"]+"/storage_engine/generated")
  cppfile = open(os.environ["EMPTYHEADED_HOME"]+"/storage_engine/codegen/Query.cpp","w")
  cppfile.write(codeString)
  cppfile.close()
  os.system("clang-format -style=llvm -i "+os.environ["EMPTYHEADED_HOME"]+"/storage_engine/codegen/Query.cpp")

def compileC(hashstring,numThreads):
  command = """sed -e "s/HASHSTRING/"""+hashstring+"""/g" """+os.environ["EMPTYHEADED_HOME"]+"/storage_engine/codegen/Query.cpp > "+os.environ["EMPTYHEADED_HOME"]+"/storage_engine/generated/Query_"+hashstring+".cpp"
  os.system(command)
  os.system("rm -rf "+os.environ["EMPTYHEADED_HOME"]+"/storage_engine/codegen/Query.cpp")
  command = """sed -e "s/HASHSTRING/"""+hashstring+"""/g" """+os.environ["EMPTYHEADED_HOME"]+"/storage_engine/codegen/Query_HASHSTRING.hpp > "+os.environ["EMPTYHEADED_HOME"]+"/storage_engine/generated/Query_"+hashstring+".hpp"
  os.system(command)
  #compile C++ code
  os.chdir(os.environ["EMPTYHEADED_HOME"]+"/storage_engine")
  try:
    os.system("make NUM_THREADS="+numThreads+" Query_"+hashstring)#+" >/dev/null")
  except:
    print "C++ compilation failed"
    sys.exit(1)

  os.chdir(os.environ["EMPTYHEADED_HOME"]+"/runtime")

def compileAndRun(f,hashstring,mem,types,annotationType,numThreads):
	generate(f,hashstring) #generates C++ code
	compileC(hashstring,numThreads) #compiles C++ library
	return cppexecutor.execute(hashstring,mem,types,annotationType) 	#generates python wrapper and excutes C++ code

def run(f,name,mem,types,annotationType):
	return cppexecutor.run(name,mem,types,annotationType) 	#generates python wrapper and excutes C++ code

def compile(f,name,mem,types,annotationType,numThreads):
	generate(f,name,hashstring) #generates C++ code
	compileC(hashstring,numThreads) #compiles C++ library
	return cppexecutor.compile(name,mem,types,annotationType) 	#generates python wrapper and excutes C++ code
