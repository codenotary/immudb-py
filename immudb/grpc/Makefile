SHELL := /bin/bash

PYTEST ?= python -m pytest
PIP ?= pip
COVERAGE ?= $(shell which coverage)

PROTO_DIR := proto
PROTO_FILE := ${PROTO_DIR}/schema.proto 
PROTO_URL := https://raw.githubusercontent.com/codenotary/immudb/master/pkg/api/schema/schema.proto
PROTOC_INCLUDE_PATH := 	-I${PROTO_DIR}

SCHEMA_OUT_DIR := immudb/schema
GRPC_OUT_DIR := immudb/service

.PHONY: ${PROTO_DIR}
${PROTO_DIR}:
	python3 -m grpc_tools.protoc \
		${PROTO_FILE} \
		--proto_path=./proto \
		${PROTOC_INCLUDE_PATH} \
		--python_out=. \
		--grpc_python_out=.

init:
	$(PIP) install -r requirements.txt --user

dev:
	$(PIP) install -r requirements-dev.txt --user

test:
	$(PYTEST) -vv --color=yes tests/

coverage:
	$(COVERAGE) run -m pytest tests

install:
	python setup.py install

.PHONY: dist
dist:
	mkdir -p ./dist
	rm ./dist/*
	python setup.py sdist bdist_wheel
