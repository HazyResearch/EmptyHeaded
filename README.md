EmptyHeaded v. 0.1
----------------------

<img src="docs/figs/eh_logo.png" height="200" >

Table of Contents
-----------------

  * [Overview](#overview)  
  * [Installing from Source](#installing-from-source)
   * [Dependencies](#dependencies)
   * [Setting up Environment](#setting-up-environment)
  * [Running Queries](#running-queries)
  * [Contact](#contact)

Overview
-----------------

Installing from Source
-----------------
To install EmptyHeaded from source ensure that your system:
- meets all dependencies detailed below
- has setup the EmptyHeaded environment

Dependencies
-----------------

* AVX
* Clang 3.6 or GCC 4.9.2
* SBT 0.13.8
* Clang-format
* jemalloc
* Intel TBB
* iPython Notebook
* C++11
* Mac or Linux operating system

The instructions below detail our dependencies and how to install them on a Linux machine with sudo priveledges. We for Mac's try `brew` (homebrew) instead of `apt-get`.

**Why AVX?**

A fundamental dependency of our system is that it is designed for machines that support the Advanced Vector Extensions (AVX) instruction set which is standard in modern and future hardware generations. Our performance is highly dependent on this instruction set being available.

**Why GCC?**

Our backend executes generated C++ code. We execute native code so that we can fully leverage the AVX instruction set.  

```
sudo add-apt-repository -y ppa:ubuntu-toolchain-r/test  
sudo apt-get update
sudo apt-get install g++-4.9
```

**Why SBT?**

Our query compiler and code generator are written using Scala.

http://www.scala-sbt.org/download.html

**Why clang-format?**

EmptyHeaded generates code from a high level datalog description. Making generated code look nice is a challenging task! Clang-format is an easy solution.

```
sudo apt-get install clang-format
```

Our system is currently tested on gcc-4.9.2 but does work with other C compilers and clang. You are free to use these at your own risk. 

**Why jemalloc?**

The GNU malloc is ineffecient for multi-threaded programs. jemalloc to the rescue!

```
sudo apt-get install libjemalloc-dev
```

**Why TBB?**

Writing an effecient parallel-sort is a challenging task. Why re-invent the wheel? Use TBB.

Linux: `sudo apt-get install libtbb-dev`

For more information....

https://www.threadingbuildingblocks.org/

**Why iPython Notebook?**

iPython Notebook provides a easy and user-friendly front-end for users to enter, compile, and run queries.

Setting up Environment
-----------------

EmptyHeaded relies on several environment variables being set.

-`EMPTYHEADED_HOME` the root directory for the EmptyHeaded project
-`EMPTYHEADED_HOME/libs` must be in the library search path
-`EMPTYHEADED_HOME/runtime` must be in the python search path

The easiest way to meet all these dependencies is to run `setup.sh` provided in the root of this repository. Note: This script will set add paths to your `LD_LIBRARY_PATH` and `PYTHON_PATH`.

Running Queries
-----------------
We demonstrate using EmptyHeaded in a example iPython notebooks. Please execute `iPython notebook` in the `EMPTYHEADED_HOME` directory and view one of our sample iPython notebooks in the `examples` directory. We are in the process of adding full support to our language but right now can only guarantee that the examples we provide run correctly right now.

Contact
-----------------
