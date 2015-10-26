import os
from distutils.core import setup, Extension
import platform
import sys

lib = sys.argv.pop(len(sys.argv)-1)
if platform.uname()[0] == "Darwin":
	clibs = ["-arch","x86_64","-std=c++0x"]
	largs = ["-arch","x86_64"]
else:
	clibs = ["-std=c++0x"]
	largs = []

EH_PATH=os.path.expandvars("$EMPTYHEADED_HOME")
# the c++ extension module
extension_mod = Extension("emptyheaded",["querywrapper.cpp"],
	include_dirs = [EH_PATH+"/storage_engine/codegen",EH_PATH+"/storage_engine/src"],
    library_dirs=[EH_PATH+"/libs"],
    libraries=[lib],
    extra_compile_args = clibs,
    extra_link_args = largs,
    )

setup(name = "emptyheaded", ext_modules=[extension_mod])

