import sys
import os
import re

logdir = os.path.expandvars("$EMPTYHEADED_HOME") + "/logs"

def get_query_times(filename):
  dataset = ""
  queryname = ""
  time = ""
  writefile = open(logdir+"/"+filename + ".csv","w")
  for line in open(filename+ ".log","r"):
    matchObj = re.match(r'.*DATASET: (.*)', line, re.M|re.I)
    if matchObj:
      dataset = matchObj.group(1)

    matchObj = re.match(r'.*QUERY: (.*)', line, re.M|re.I)
    if matchObj:
      queryname = matchObj.group(1)
    
    matchObj = re.match(r'.*Time\[QUERY TIME\]: (\d+.\d+) s.*', line, re.M|re.I)
    if matchObj:
      first = True
      time = matchObj.group(1)
    if time != "" and queryname != "":
      writefile.write(dataset + "," + queryname + "," + time + "\n")
      queryname = ""
      time = ""
  return -1.0


def main():
  os.system("rm -rf " + logdir)
  os.system("mkdir -p " + logdir)  
  get_query_times("regression")

if __name__ == "__main__": main()
