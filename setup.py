#!/usr/bin/env python
# Copyright 2021 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()
    
setup(name='immudb-py',
      version='0.9.1rc1',
      license="Apache License Version 2.0",
      description='Python SDK for Immudb',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='vChain',
      url='https://github.com/codenotary/immu-py',
      #download_url='',
      packages=['immudb', 'immudb.handler', 'immudb.grpc' ],
      keywords=['immudb', 'immutable'],
      install_requires=[
        'grpcio>=1.26.0',
        'dataclasses>=0.6',
        'protobuf>=3.13.0',
        'google-api>=0.1.12',
        'google-api-core>=1.22.0'
        ],
      classifiers=[
              'Intended Audience :: Developers',
              'Topic :: Software Development :: Build Tools',
              "License :: OSI Approved :: Apache Software License",
              "Operating System :: OS Independent",
              'Programming Language :: Python :: 3',
              'Programming Language :: Python :: 3.6',
              'Programming Language :: Python :: 3.7',
              'Programming Language :: Python :: 3.8',
              ],
      python_requires='>=3.6',
      )
