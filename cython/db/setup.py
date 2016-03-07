from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize
import sys
import numpy
import platform
import os

EH_PATH=os.path.expandvars("$EMPTYHEADED_HOME")
if platform.uname()[0] == "Darwin":
  clibs = ["-arch","x86_64","-mavx",'-Wno-unused-function',
            '-stdlib=libc++',
            '-std=c++11',
            '-mmacosx-version-min=10.8',]
  largs = ["-arch","x86_64"]
else:
  clibs = ["-std=c++0x"]
  largs = ["-Wl,--Bshareable"]

extensions = [
    Extension(
        "DB",
        ["DB.pyx"],
        include_dirs = [numpy.get_include()],
        extra_compile_args = clibs,
        extra_link_args = largs,
        language="c++"
    )
]
setup(
    name='DB',
    ext_modules=cythonize(
        extensions,
    )
)