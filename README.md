EmptyHeaded v. 0.1
----------------------

<img src="docs/figs/cct_logo.png" height="200" >

# Dependencies

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


# Installing from Source

One all the dependencies above are met one can simply run `source setup.sh.` This script sets up environment variables, adds EmptyHeaded to your LD_LIBRARY_PATH, compiles the storage engine, and compiles query compiler. After executing this script you are ready to run your try your first EmptyHeaded query.

# Running Queries

We demonstrate using EmptyHeaded in a example iPython notebooks. Please execute `iPython notebook` in the `EMPTYHEADED_HOME` directory and view one of our sample iPython notebooks in the `examples` directory. We are in the process of adding full support to our language but right now can only guarantee that the examples we provide run correctly right now.
