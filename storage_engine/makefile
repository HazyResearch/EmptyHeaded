override CXXFLAGS += -mavx -std=c++0x -pedantic -O3 -Wall -Wno-unused-function -Wextra -march=native -mtune=native

OBJDIR=build
EXEDIR=bin
HEADERS=$(wildcard include/*hpp)
SOURCES=$(wildcard src/queryplan/*cpp)
OBJECTS=$(SOURCES:src/queryplan/%.cpp=$(OBJDIR)/%.o)

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

ifdef CLANG
	CXX = clang++
else 
	CXX = g++
endif

APPS_SOURCES=$(shell ls apps)
APPS=$(APPS_SOURCES:.cpp=)
APPS_EXES=$(APPS:%=$(EXEDIR)/%)

LIBS=-ltbb -lpthread

all: $(APPS_EXES) $(TOOLS_EXES) $(SERVER_EXES)

$(OBJDIR):
	mkdir -p $(OBJDIR)

$(EXEDIR):
	mkdir -p $(EXEDIR)

$(APPS_EXES): $(EXEDIR)
	$(CXX) $(CXXFLAGS) $(@:bin%=apps%.cpp) $(LIBS) -L$(OBJDIR) -lemptyheaded -o $@ -Isrc/queryplan

$(OBJECTS): $(SOURCES) $(HEADERS) $(OBJDIR)
	$(CXX) -fPIC $(CXXFLAGS) $(LIB_INCS) -o $@ -c $(@:build%.o=src/queryplan%.cpp)

codegen:
	$(CXX) -fPIC $(CXXFLAGS) $(LIB_INCS) -o $(OBJDIR)/Query.o -c src/codegen/Query.cpp -Isrc/queryplan
	$(CXX) -shared -o lib$(@).so $(OBJDIR)/Query.o -L$(OBJDIR) -lemptyheaded $(LIBS)
	mv *.so ../python

emptyheaded: $(OBJDIR) $(OBJECTS)
	ar rvs $(OBJDIR)/lib$(@).a $(OBJECTS)
	$(CXX) -shared -o lib$(@).so $(OBJECTS) $(LIBS)

tests: $(TESTS_EXES)

clean:
	rm -rf $(OBJDIR) $(EXEDIR)

.PHONY: all libs tests clean
