# EmptyHeaded

# Dependencies

* Clang-format
* Clang 3.6 or GCC 4.9.2
* jemalloc
* SBT 0.13.8
* Intel TBB
* C++11
* AVX

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


# Installing from Source

One all the dependencies about are met you can install EmptyHeaded from source following the steps below.

1) Setting up the EmptyHeaded environment.

Once the source code is downloaded execute `source setupEnv.sh` in the top level of this project. The top level of the project is set to an environment variable EMPTYHEADED_HOME via this script. 

This script compiles a shared library (.so) for the EmptyHeaded storage engine and places this in `EMPTYHEADED_HOME/libs`. Then `EMPTYHEADED_HOME/libs` is next added to your `LD_LIBRARY_PATH`.  
