#!/usr/bin/env python3
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
      version='1.5.0',
      license="Apache License Version 2.0",
      description='Python SDK for Immudb',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='Codenotary',
      url='https://github.com/codenotary/immudb-py',
      # download_url='',
      packages=['immudb', 'immudb.database', 'immudb.embedded',
                'immudb.embedded.ahtree', 'immudb.embedded.htree', 'immudb.embedded.store',
                'immudb.grpc', 'immudb.handler', 'immudb.schema'],
      keywords=['immudb', 'immutable'],
      install_requires=[
          'appier>=1.32.0',
          'cachetools>=5.3.3',
          'certifi>=2024.2.2',
          'charset-normalizer>=3.3.2',
          'ecdsa>=0.18.0',
          'google-api>=0.1.12',
          'google-api-core>=2.18.0',
          'google-api-python-client>=2.123.0',
          'google-auth>=2.29.0',
          'google-auth-httplib2>=0.2.0',
          'googleapis-common-protos>=1.63.0',
          'grpcio>=1.62.1',
          'httplib2>=0.22.0',
          'idna>=3.6',
          'proto-plus>=1.23.0',
          'protobuf>=4.25.3',
          'pyasn1>=0.6.0',
          'pyasn1_modules>=0.4.0',
          'pyparsing>=3.1.2',
          'requests>=2.31.0',
          'rsa>=4.9',
          'six>=1.16.0',
          'uritemplate>=4.1.1',
          'urllib3>=2.2.1',
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
      python_requires='>=3.8',
      )
