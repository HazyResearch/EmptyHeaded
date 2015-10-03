import os
import relation.relation

def loadAndRunQuery(name):
    name = "queries." + name
    mod = __import__(name, fromlist=[''])
    return mod.run()

def compileQuery(name):
	os.chdir("wrapper")
	os.system("./build.sh " + name + " >/dev/null 2>&1")
	os.chdir("..")

def main():
	compileQuery("query1")
 	a = loadAndRunQuery("query1")
 	print relation.relation.num_rows(a)

if __name__ == "__main__": main()