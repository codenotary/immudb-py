#!/usr/bin/env python

from distutils.core import setup

setup(name='immu-py',
      version='1.0',
      description='Python SDK for Immudb',
      author='vChain',
      url='https://github.com/codenotary/immu-py',
      packages=['immudb', 'immudb.handler', 'immudb.service', 'immudb.schema'],
      install_requires=[
        'grpcio>=1.26.0',
        'dataclasses>=0.6',
        'protobuf>=3.13.0',
        'google-api>=0.1.12',
        'google-api-core>=1.22.0'
        ])
