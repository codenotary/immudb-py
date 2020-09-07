#!/usr/bin/env python

from distutils.core import setup

setup(name='immu-py',
      version='1.0',
      description='Python SDK for Immudb',
      author='vChain',
      url='https://github.com/codenotary/immu-py',
      packages=['immu', 'immu.handler', 'immu.service', 'immu.schema'],
      install_requires=[
        'grpcio>=1.26.0',
        'dataclasses>=0.6',
        'protobuf>=3.13.0'
        ])
