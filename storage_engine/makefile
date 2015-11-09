override CXXFLAGS += -mavx -std=c++0x -pedantic -O3 -Wall -Wno-unused-function -Wextra -march=native -mtune=native

OBJDIR=build
EXEDIR=bin
HEADERS=$(wildcard include/*hpp)
SOURCES=$(shell find src -name "*cpp")
OBJECTS=$(SOURCES:src/%.cpp=$(OBJDIR)/%.o)

ifdef DEBUG
	override CXXFLAGS += -DDEBUG -g
endif

ifdef NO_VECTOR
	override CXXFLAGS += -fno-tree-vectorize -DVECTORIZE=0
else
	override CXXFLAGS += -DVECTORIZE=1
endif

ifdef NO_ALGORITHM
	override CXXFLAGS += -DNO_ALGORITHM=1
endif

ifdef NUM_THREADS
	override CXXFLAGS += -DNUM_THREADS_IN=$(NUM_THREADS)
endif

LINKERARGS=-Wl,-rpath=$(EMPTYHEADED_HOME)/storage_engine/libs
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
CXX = g++-4.9
endif
ifeq ($(UNAME_S),Darwin)
CXX = clang++
override LINKERARGS=
endif

ifdef NUM_THREADS
	override CXXFLAGS += -DNUM_THREADS_IN=$(NUM_THREADS)
endif

APPS_SOURCES=$(shell ls apps)
APPS=$(APPS_SOURCES:.cpp=)
APPS_EXES=$(APPS:%=$(EXEDIR)/%)

$(shell mkdir -p generated)
CODEGEN_SOURCES=$(shell ls generated)
CODEGEN=$(CODEGEN_SOURCES:.cpp=)
CODEGEN_EXES=$(CODEGEN:%=$(EXEDIR)/%)

LIBS=-ljemalloc -ltbb -lpthread

all: $(APPS_EXES) $(TOOLS_EXES) $(SERVER_EXES)

$(OBJDIR):
	mkdir -p $(OBJDIR)
	mkdir -p $(OBJDIR)/utils

libdir:
	mkdir -p libs

$(EXEDIR):
	mkdir -p $(EXEDIR)

$(APPS_EXES): $(EXEDIR) emptyheaded
	$(CXX) $(CXXFLAGS) $(@:bin%=apps%.cpp) $(LIBS) -Llibs -lemptyheaded -o $@ -Isrc

query: $(EXEDIR) emptyheaded
	$(CXX) $(CXXFLAGS) -DEXECUTABLE=1 codegen/Query.cpp $(LIBS) -Llibs -lemptyheaded -o bin/query -Isrc

$(CODEGEN_EXES): $(EXEDIR) emptyheaded
	$(CXX) $(CXXFLAGS) -DEXECUTABLE=1 $(@:bin%=generated%.cpp) $(LIBS) -Llibs -lemptyheaded -o $@ -Isrc

$(OBJECTS): $(SOURCES) $(HEADERS) $(OBJDIR)
	$(CXX) -fPIC $(CXXFLAGS) $(LIB_INCS) -o $(@) -c $(@:build%.o=src%.cpp)

$(CODEGEN):
	$(CXX) -fPIC $(CXXFLAGS) $(LIB_INCS) -o $(OBJDIR)/$(@).o -c generated/$(@).cpp -Isrc
	$(CXX) -shared $(LINKERARGS) -o $(EMPTYHEADED_HOME)/runtime/queries/lib$(@).so $(OBJDIR)/$(@).o -L$(EMPTYHEADED_HOME)/storage_engine/libs/ -lemptyheaded $(LIBS)

emptyheaded: $(OBJDIR) $(OBJECTS) libdir
	$(CXX) -shared -o $(EMPTYHEADED_HOME)/storage_engine/libs/lib$(@).so $(OBJECTS) $(LIBS)

tests: $(TESTS_EXES)

clean:
	rm -rf $(OBJDIR) $(EXEDIR)
	rm -rf $(EMPTYHEADED_HOME)/storage_engine/libs
	rm -rf $(EMPTYHEADED_HOME)/runtime/queries/*.so

.PHONY: all libs tests clean
