import cppexecutor
import os
import querytemplate

def compileAndRun(f,name):
	#generate C++ code
	code = f()
	os.system("mkdir -p ../storage_engine/generated")
	cppfile = open("../storage_engine/generated/"+name+".cpp","w")
	cppfile.write(code)
	cppfile.close()

	#compile C++ code
	os.chdir("../storage_engine/codegen")
	os.system("ln -sfn ../generated/"+name+".cpp Query.cpp")
	os.chdir("..")
	os.system("make cgen >/dev/null 2>&1")
	os.chdir("../python")

	#generates python wrapper and excutes C++ code
	cppexecutor.execute(name)


##########
#Code generation methods
def loadRelationCode():
	return """
  std::cout << "in FUCK land" << std::endl;
  GHD* a = new GHD();
  auto ret_result = a->run();
  num_rows = std::get<0>(ret_result);
  result = (Trie<A>*)std::get<1>(ret_result);
 """

def loadRelation(relation,env):
	runCode = loadRelationCode()
	return querytemplate.getCode(runCode)

def buildTrie(ordering,relation,env):
	print "codegen"

def nprr():
	print "nprr"

def top_down():
	print "top down"
