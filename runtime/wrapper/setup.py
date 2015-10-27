import os
from distutils.core import setup, Extension
import platform
import sys

EH_PATH=os.path.expandvars("$EMPTYHEADED_HOME")
lib = sys.argv.pop(len(sys.argv)-1)
if platform.uname()[0] == "Darwin":
	clibs = ["-arch","x86_64","-std=c++0x"]
	largs = ["-arch","x86_64"]
	largs += ["-L"+EH_PATH+"/runtime/queries/","-L"+EH_PATH+"/storage_engine/libs/"]
else:
	clibs = ["-std=c++0x"]
	largs = ["-Wl,-rpath="+EH_PATH+"/runtime/queries"]

# the c++ extension module
extension_mod = Extension("emptyheaded",["querywrapper.cpp"],
	include_dirs = [EH_PATH+"/storage_engine/generated",EH_PATH+"/storage_engine/src"],
    library_dirs=[EH_PATH+"/runtime/queries"],
    libraries=[lib],
    extra_compile_args = clibs,
    extra_link_args = largs,
    )

setup(name = "emptyheaded", ext_modules=[extension_mod])

