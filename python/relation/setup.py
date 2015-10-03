import os
from distutils.core import setup, Extension

os.environ["CC"] = "g++"

# the c++ extension module
extension_mod = Extension("relation",["relation.cpp"],
	include_dirs = ["../../storage_engine/src/codegen/"],
    library_dirs=[".."],
    libraries=["codegen"],
    extra_compile_args = ["-arch","x86_64",],
    extra_link_args = ["-arch","x86_64"],
    )

setup(name = "relation", ext_modules=[extension_mod])

