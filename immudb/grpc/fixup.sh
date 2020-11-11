#!/bin/sh
sed -i -r 's/^import (.+_pb2.*)/from . import \1/g' *_pb2*.py
