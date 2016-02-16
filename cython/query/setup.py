import os
import platform
import sys
import numpy
from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

EH_PATH=os.path.expandvars("$EMPTYHEADED_HOME")
if platform.uname()[0] == "Darwin":
  clibs = ["-arch","x86_64","-std=c++0x"]
  largs = ["-arch","x86_64"]

extensions = [
    Extension("Query", ["Query.pyx"],
        include_dirs = [EH_PATH+"/storage_engine/include",numpy.get_include()],
        libraries = ["emptyheaded"],
        library_dirs = [EH_PATH+"/storage_engine/build/lib"],
        language="c++",
        extra_compile_args = clibs,
        extra_link_args = largs,
    )
]

setup(
    ext_modules = cythonize(extensions)
)