import emptyheaded 

class ResultError(Exception):
    pass

def main():
  db_config="$EMPTYHEADED_HOME/examples/fft/db"
  emptyheaded.loadDB(db_config)
  print emptyheaded.fetchData("onelevel")
  print emptyheaded.fetchData("twolevel")

if __name__ == "__main__": main()
