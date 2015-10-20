override CXXFLAGS += -mavx -std=c++0x -pedantic -O3 -Wall -Wno-unused-function -Wextra -march=native -mtune=native

OBJDIR=build
EXEDIR=bin
HEADERS=$(wildcard include/*hpp)
SOURCES=$(shell find src -name "*cpp")
#STRIPPED=$(notdir $(SOURCES))
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

ifdef CLANG
	CXX = clang++
else 
	CXX = g++
endif

APPS_SOURCES=$(shell ls apps)
APPS=$(APPS_SOURCES:.cpp=)
APPS_EXES=$(APPS:%=$(EXEDIR)/%)

$(shell mkdir -p generated)
CODEGEN_SOURCES=$(shell ls generated)
CODEGEN=$(CODEGEN_SOURCES:.cpp=)
$(info $(CODEGEN))
CODEGEN_EXES=$(CODEGEN:%=$(EXEDIR)/%)

LIBS=-ltbb -lpthread

all: $(APPS_EXES) $(TOOLS_EXES) $(SERVER_EXES)

$(OBJDIR):
	mkdir -p $(OBJDIR)
	mkdir -p $(OBJDIR)/utils

libdir:
	mkdir -p ../libs

$(EXEDIR):
	mkdir -p $(EXEDIR)

$(APPS_EXES): $(EXEDIR) emptyheaded
	$(CXX) $(CXXFLAGS) $(@:bin%=apps%.cpp) $(LIBS) -L$(OBJDIR) -lemptyheaded -o $@ -Isrc

$(CODEGEN_EXES): $(EXEDIR) emptyheaded
	$(CXX) $(CXXFLAGS) -DEXECUTABLE=1 $(@:bin%=generated%.cpp) $(LIBS) -L$(OBJDIR) -lemptyheaded -o $@ -Isrc

$(OBJECTS): $(SOURCES) $(HEADERS) $(OBJDIR)
	$(CXX) -fPIC $(CXXFLAGS) $(LIB_INCS) -o $(@) -c $(@:build%.o=src%.cpp)

$(CODEGEN):
	$(CXX) -fPIC $(CXXFLAGS) $(LIB_INCS) -o $(OBJDIR)/$(@).o -c generated/$(@).cpp -Isrc
	$(CXX) -shared -o lib$(@).so $(OBJDIR)/$(@).o -L$(OBJDIR) -lemptyheaded $(LIBS)
	rm -rf $(OBJDIR)/$(@).o
	mv lib$(@).so ../libs/.

emptyheaded: $(OBJDIR) $(OBJECTS) libdir
	#ar rvs $(OBJDIR)/lib$(@).a $(OBJECTS)
	$(CXX) -shared -o lib$(@).so $(OBJECTS) $(LIBS)
	cp -f lib$(@).so ../libs/.
	mv lib$(@).so $(OBJDIR)/.

tests: $(TESTS_EXES)

clean:
	rm -rf $(OBJDIR) $(EXEDIR)
	rm -rf ../libs/emptyheaded.so

.PHONY: all libs tests clean
