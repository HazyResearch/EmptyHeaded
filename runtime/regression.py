import emptyheaded
import sys
import os
import re

logdir = os.path.expandvars("$EMPTYHEADED_HOME") + "/logs"

def get_query_times(filename):
  dataset = ""
  queryname = ""
  time = ""
  writefile = open(logdir+"/"+filename + ".csv","w")
  for line in open(logdir+"/"+filename+ ".log","r"):
    matchObj = re.match(r'.*DATASET: (.*)', line, re.M|re.I)
    if matchObj:
      dataset = matchObj.group(1)

    matchObj = re.match(r'.*RUNNING QUERY: (.*)', line, re.M|re.I)
    if matchObj:
      queryname = matchObj.group(1)
    
    if queryname == "PageRank":
      matchObj = re.match(r'.*Time\[QUERY TIME\]: (\d+.\d+) s.*', line, re.M|re.I)

    matchObj = re.match(r'.*Time\[QUERY TIME\]: (\d+.\d+) s.*', line, re.M|re.I)
    if matchObj:
      time = matchObj.group(1)

    if time != "" and queryname != "":
      writefile.write(dataset + "," + queryname + "," + time + "\n")
      queryname = ""
      time = ""
  return -1.0

def main():
  os.system("rm -rf " + logdir)
  os.system("mkdir -p " + logdir)  

  datasets = ["googlePlus","higgs","socLivejournal","orkut","cidPatents","twitter2010"]
  for dataset in datasets:
    os.system("cd " + logdir + " && python "+os.path.expandvars("$EMPTYHEADED_HOME")+"/runtime/emptyheaded.py pruned "+dataset+" |& tee --append "+logdir+"/pruned.log")
  get_query_times("pruned")

  for dataset in datasets:
    command="cd " + logdir + " && python "+os.path.expandvars("$EMPTYHEADED_HOME")+"/runtime/emptyheaded.py duplicated "+dataset+" |& tee --append "+logdir+"/duplicated.log"
    os.system(command)
  get_query_times("duplicated")

  os.system("cd " + logdir + " && python "+os.path.expandvars("$EMPTYHEADED_HOME")+"/runtime/emptyheaded.py lubm |& tee "+logdir+"/lubm10000.log")
  get_query_times("lubm10000")

if __name__ == "__main__": main()
