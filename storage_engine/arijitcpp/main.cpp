#include "loadAndEncode.hpp"
#include "run_0.hpp"

void createDB(){
  char* ehPath;
  ehPath = getenv ("EMPTYHEADED_HOME");
  if (ehPath!=NULL)
    printf ("The EMPTYHEADED_HOME path is: %s\n",ehPath);

  char* dbPath;
  dbPath = getenv ("EMPTYHEADED_DB_PATH");
  if (dbPath!=NULL)
    printf ("The EMPTYHEADED_DB_PATH path is: %s\n",dbPath);

  //input tsv file.
  std::string graph_source=std::string(ehPath)+"/test/graph/data/facebook_duplicated.tsv";
  loadAndEncode(graph_source,dbPath);
}

void run(){
  char* ehPath;
  ehPath = getenv ("EMPTYHEADED_HOME");
  if (ehPath!=NULL)
    printf ("The EMPTYHEADED_HOME path is: %s\n",ehPath);

  char* dbPath;
  dbPath = getenv ("EMPTYHEADED_DB_PATH");
  if (dbPath!=NULL)
    printf ("The EMPTYHEADED_DB_PATH path is: %s\n",dbPath);

  run_0(dbPath);
}


int main(){
  createDB();
  run();
  return 0;
}