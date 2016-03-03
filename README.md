EmptyHeaded v. 0.1
----------------------

<img src="docs/figs/eh_logo.png" height="200" >

[![Build Status](https://travis-ci.org/HazyResearch/EmptyHeaded.svg?branch=master)](https://travis-ci.org/HazyResearch/EmptyHeaded)

Table of Contents
-----------------

  * [Overview](#overview)  
  * [Installing from Source](#installing-from-source)
   * [Dependencies](#dependencies)
   * [Docker Containers](#docker)
   * [Setting up Environment](#setting-up-environment)
   * [Compilation](#compilation)
  * [Running Queries](#running-queries)
  * [Contact](#contact)

Overview
-----------------

EmptyHeaded is a new style of join processing engine. A full description is in our [manuscript](http://arxiv.org/abs/1503.02368). A brief overview is as follows.

Joins are ubiqutous in data processing and often are the bottleneck of classic RDBMS workloads. Recent database theory has shown that Sellinger style join optimizers, which compute joins in a pairwise fashion are asympotically suboptimal. Therefore the join optimizers which have dominated the RDBMS for the past 40 years run suboptimal algorithms. Ngo et al. showed a new way to compute joins in a multiway fashion in this [paper](http://arxiv.org/abs/1203.1952). EmptyHeaded extends this line of work with novel theoretical and systems advances. Namely, in our query compiler we use a new worst-case optimal join algorithm (with stronger guarantees) that leverages [new theoretical advances for aggregate join queries](http://arxiv.org/abs/1508.07532).  We are also the first to translate this new theory into a system that leverages bit-level parallelism at several granularities within the data. Our storage engine accomplishes this through its design for SIMD parallelism.

EmptyHeaded is designed to run as a python library. Behind the scenes there are three pieces of the EmptyHeaded engine.

1. Query Compiler
2. Code Generator
3. Storage Engine

The query compiler and code generator are written in Scala and the storage engine is written in C++. The query compiler accepts a datalog string and produces a generalized hypertree decomposition (GHD) which represents our query plan. The code generator takes in the GHD from the query compiler and generates C++ library calls in the storage engine. This code is then compiled and run.

Installing from Source
-----------------
To install EmptyHeaded from source ensure that your system:
1. Meets all [dependencies](#dependencies) detailed below (or you are in our [Docker](#docker) contatiner)
2. Has [setup the EmptyHeaded environment](#setting-up-environment)
3. Has [compiled the QueryCompiler and Cython bindings](#compilation).

Dependencies
-----------------
Behind the scenes a lot goes on in our engine. This is no walk in the park library based engine--we have advanced theoretical compilation techniques, code generation, and highly optimized code for hardware--all spanning multiple programming languages. As such we have a fair number of dependencies. Try using our [Docker images](#docker) where everything is taken care of for you already.

**AVX**

A fundamental dependency of our system is that it is designed for machines that support the Advanced Vector Extensions (AVX) instruction set which is standard in modern and future hardware generations. Our performance is highly dependent on this instruction set being available. We currenlty DO NOT support old hardware generations without AVX. 

* Mac or Linux operating system
* GCC 5.3 (Linux) or Apple LLVM version 7.0.2 (Mac)
* clang-format
* C++11 
* cmake 2.8 or higher (C++)
* jemalloc (C++)
* tbb (C++, Intel)
* sbt (scala)
* iPython Notebook (python)
* cython (python)
* jpype 0.6.1 (python)
* pandas (python)

The instructions below briefly describe some of our dependencies and why we have them. A complete list of our dependencies as well as how to install them is in our `Dockerfile`. Note: we provide JPype and an install script in our `dependencies` folder.

**Why iPython Notebook?**

iPython Notebook provides a easy and user-friendly front-end for users to enter, compile, and run queries.

**Why clang-format?**

EmptyHeaded generates code from a high level datalog description. Making generated code look nice is a challenging task! [Clang-format](http://clang.llvm.org/docs/ClangFormat.html) is an easy solution.

**Why jemalloc?**

The GNU malloc is ineffecient for multi-threaded programs. [jemalloc](https://www.facebook.com/notes/facebook-engineering/scalable-memory-allocation-using-jemalloc/480222803919/) to the rescue!

**Why TBB?**

Writing an efficient parallel-sort is a challenging task. Why re-invent the wheel? Use [Intel's TBB](https://www.threadingbuildingblocks.org/).

**Why Pandas?**

[Pandas DataFrames](http://pandas.pydata.org/pandas-docs/stable/dsintro.html) provides a nice and highly used front-end for EmptyHeaded to accept tables from. We can also run without DataFrames but who doesn't love DataFrames?

**Why JPype?**

JPype is our bridge between python and java. We provide this one in our `dependencies` folder along with a simple install script.

Docker
-----------------
Make your life easier and use our [Docker images](https://hub.docker.com/r/craberger/emptyheaded/) which are *always* up to date. 

Unfortunately iPython notebooks and Docker containers do not interact easily, but you can run standard python scripts just fine in these containers! 

Two easy ways to get started in a container:
1. Simply inspect our iPython notebook tutorials in this repository (can view on github) and make the corresponding python programs. 
2. Checkout our python test scripts in the `test` folder, `./test/testAll.sh` kick them it all off.

Setting up Environment
-----------------

```
source env.sh
```

EmptyHeaded relies on two environment variables being set.

-`EMPTYHEADED_HOME` the root directory for the EmptyHeaded project

-`EMPTYHEADED_HOME/python` must be in the python search path

The easiest way to meet these dependencies is to run `source env.sh` provided in the root of this repository. Note: This script will set the `PYTHON_PATH` variable.

Compilation
-----------------

```
./compile.sh
```

The compiler needs to be compiled (but who compiled the first compiler?). This compiles our Scala code and a few static cython bindings.

Running Queries
-----------------
We provide demos of EmptyHeaded in iPython notebooks. 

We provide a tutorial of how to get started running your first EmptyHeaded query in the `examples/graph` folder. In this folder is a `Graph Tutorial` iPython notebook which can be loaded after executing `iPython notebook`

Contact
-----------------

[Christopher Aberger](http://web.stanford.edu/~caberger/)
