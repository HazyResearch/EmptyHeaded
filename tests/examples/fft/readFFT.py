import emptyheaded 

class ResultError(Exception):
    pass

def main():
  db_config="$EMPTYHEADED_HOME/examples/fft/db"
  emptyheaded.loadDB(db_config)
  row3 = emptyheaded.fetchData("onelevel").iloc[3]
  print row3["annotation"]
  if abs(row3["annotation"] - 1.3) > 0.0001:
    raise ResultError("ROW3 INCORRECT: " + str(row3))

  row12 = emptyheaded.fetchData("twolevel").iloc[12]
  print row12["annotation"]
  if abs(row12["annotation"] - 88.8) > 0.0001:
    raise ResultError("ROW3 INCORRECT: " + str(row12))

if __name__ == "__main__": main()
