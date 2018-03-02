# Copyright (c) 2015 MaxPoint Interactive, Inc.
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
# following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
#    disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
#    products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from setuptools import setup
from setuptools.extension import Extension, Library
from Cython.Build import cythonize
import numpy as np
import os
import platform

#import Cython.Compiler.Options
#Cython.Compiler.Options.annotate = True


def _get_include(prefix):
    return [os.path.join(prefix, 'include')], [os.path.join(prefix, 'lib')]


def join_root(path):
    return os.path.join(os.path.dirname(__file__), path)


# This is needed by conda build
if 'PREFIX' in os.environ:
    print("Operating setup.py from within conda-build")
    include_dirs, library_dirs = _get_include(os.environ['PREFIX'])
elif 'CONDA_PREFIX' in os.environ:
    print("Operating setup.py from within a conda environment")
    include_dirs, library_dirs = _get_include(os.environ['CONDA_PREFIX'])
else:
    include_dirs = []
    library_dirs = []

include_dirs.append(np.get_include())

extensions = [
    Extension(
        name='cyavro._cyavro',
        sources=['cyavro/_cyavro.pyx'],
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=['avro', 'm', 'snappy'],
    ),
    Extension(
        name='cyavro.test_utils',
        sources=['cyavro/test_utils.pyx'],
        include_dirs=include_dirs,
        library_dirs=library_dirs,
        libraries=['avro', 'm', 'snappy'],
    )
]

if platform.system() == "Darwin":
    for e in extensions:
        e.sources.append("cyavro/osx/fmemopen.c")
        e.depends.append('cyavro/osx/fmemopen.h')

version = str(os.environ.get('PKG_VERSION', "0.7.0"))


def write_version_py():
    content = """\
version = '%s'
""" % version

    filename = os.path.join(os.path.dirname(__file__), 'cyavro', 'version.py')
    with open(filename, 'w') as fo:
        fo.write(content)

write_version_py()


setup(name='cyavro',
      version=version,
      packages=['cyavro'],
      package_data={'cyavro': ['_cyavro.c', '*.pyx', '*.pxd']},
      description='Wrapper to avro-c-1.7+',
      maintainer='Valassis Digital',
      maintainer_email='marius.v.niekerk@gmail.com',
      author='Valassis Digital',
      author_email='marius.v.niekerk@gmail.com',
      requires=['pandas (>=0.12)',
                'numpy (>=1.7.0)',
                'cython (>=0.19.1)',
                'six'],
      ext_modules=cythonize(extensions),
      url='https://github.com/Valassis-Digital-Media/cyavro',
      license='BSD',
      )
