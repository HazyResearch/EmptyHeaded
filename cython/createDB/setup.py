import os
import platform
import sys
import numpy
from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

EH_PATH=os.path.expandvars("$EMPTYHEADED_HOME")
STORAGE_LIBRARY_DIR=os.environ.get(
  "EMPTYHEADED_STORAGE_LIBRARY_DIR", EH_PATH+"/storage_engine/build/lib")
if platform.uname()[0] == "Darwin":
  clibs = ["-arch","x86_64","-mavx",'-Wno-unused-function',
            '-stdlib=libc++',
            '-std=c++11',
            '-mmacosx-version-min=10.8',]
  largs = ["-arch","x86_64"]
else:
  clibs = ["-std=c++0x"]
  largs = ["-Wl,-rpath="+STORAGE_LIBRARY_DIR,"-Wl,--Bshareable"]
  os.environ["CC"] = "g++-5" 
  os.environ["CXX"] = "g++-5"

extensions = [
    Extension(
        "#DFMap#",
        ["#DFMap#.pyx"],
        libraries = ["emptyheaded"],
        library_dirs = [STORAGE_LIBRARY_DIR],
        include_dirs = [EH_PATH+"/storage_engine/include",numpy.get_include()],
        extra_compile_args = clibs,
        extra_link_args = largs,
        language="c++"
    )
]
setup(
    name='#PTrie#',
    ext_modules=cythonize(
        extensions,
    )
)


