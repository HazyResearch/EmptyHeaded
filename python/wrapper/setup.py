import os
from distutils.core import setup, Extension

os.environ["CC"] = "g++"

# the c++ extension module
extension_mod = Extension("emptyheaded",["querywrapper.cpp"],
	include_dirs = ["../../storage_engine/codegen","../../storage_engine/src"],
    library_dirs=[".."],
    libraries=["cgen"],
    extra_compile_args = ["-arch","x86_64","-std=c++11"],
    extra_link_args = ["-arch","x86_64"],
    )

setup(name = "emptyheaded", ext_modules=[extension_mod])

