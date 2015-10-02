#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "ghd.hpp"
#include "emptyheaded.hpp"

char * query(char * name)
{
  char * buf;
  buf = (char *) malloc(strlen("I just ran the triangle query, ") + strlen(name) + 1);

  printf("calling eh library");

  GHD a;
  a.run();

  sprintf(buf, "I just ran the triangle query, %s", name);
  return buf;
}