from setuptools import setup

with open("README.md", 'r') as f:
    long_description = f.read()

setup(
  name='immu',
  version='1.0',
  description='ImmuDB python driver',
	long_description=long_description,
  author='Hextar',
  packages=['immu', 'immu.schema', 'immu.service'],
  install_requires=['grpcio'],
	license='Apache License 2.0'
)