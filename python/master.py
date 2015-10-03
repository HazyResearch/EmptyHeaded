import os

def loadQuery(name):
    name = "queries." + name
    mod = __import__(name, fromlist=[''])
    return mod

def compileQuery(name):
	os.chdir("wrapper")
	os.system("./build.sh " + name + ">/dev/null 2>&1")
	os.chdir("..")

def main():
	compileQuery("query1")
 	q1 = loadQuery("query1")
 	q1_result = q1.run()
 	print q1.num_rows(q1_result)
 	q1.fetch_data(q1_result)


if __name__ == "__main__": main()