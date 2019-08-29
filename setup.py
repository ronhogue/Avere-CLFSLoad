#
# setup.py
#
#-------------------------------------------------------------------------
# Copyright (c) Microsoft.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#--------------------------------------------------------------------------

from __future__ import absolute_import

import importlib
import logging
import setuptools

def doit():
    logging.basicConfig(level=logging.WARNING)

    spec = importlib.util.spec_from_file_location('types', 'clfsload/version.py')
    version = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(version)

    requirements = list()
    with open("requirements.txt", "r") as f:
        for line in f.readlines():
            line = line.strip()
            if (not line) or line.startswith('#'):
                continue
            requirements.append(line)

    desc_short = 'Avere CLFSLoad ingest Python library and CLFSLoad.py command line utility'

    desc_long = 'The ' + desc_short \
      + ' provides the ability to ingest data from a rooted source' \
      + ' into an Azure container in the CLFS format.' \
      + ' Licensed under the Apache License, Version 2.0.'

    setuptools.setup(author='Microsoft Corporation',
                     classifiers=['Development Status :: 4 - Beta', # XXXjimz
                                  'Environment :: Console',
                                  'Intended Audience :: System Administrators',
                                  'License :: OSI Approved :: Apache Software License',
                                  'Operating System :: POSIX :: Linux',
                                  'Programming Language :: Python :: 3',
                                 ],
                     description=desc_short,
                     entry_points={'console_scripts' : ['CLFSLoad.py = clfsload.core:main']},
                     install_requires=requirements,
                     keywords='Avere Microsoft',
                     license='Apache 2.0',
                     long_description=desc_long,
                     name='clfsload',
                     packages=['clfsload'],
                     python_requires='~=3.6',
                     version=version.VERSION_STRING)

doit()
