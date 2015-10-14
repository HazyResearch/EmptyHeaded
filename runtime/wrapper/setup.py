import os
from distutils.core import setup, Extension
import platform
import sys

lib = sys.argv.pop(len(sys.argv)-1)
if platform.uname()[0]:
	clibs = ["-arch","x86_64","-std=c++11"]
	largs = ["-arch","x86_64"]
else:
	clibs = ["-std=c++11"]
	largs = []
	
# the c++ extension module
extension_mod = Extension("emptyheaded",["querywrapper.cpp"],
	include_dirs = ["../../storage_engine/codegen","../../storage_engine/src"],
    library_dirs=["../../libs"],
    libraries=[lib],
    #extra_compile_args = ["-std=c++11"],
    extra_compile_args = clibs,
    extra_link_args = largs,
    )

setup(name = "emptyheaded", ext_modules=[extension_mod])

