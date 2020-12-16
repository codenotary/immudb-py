SHELL := /bin/bash

PROTOC ?= $(shell which protoc)
GRPC_PYTHON_PLUGIN ?= $(shell which grpc_python_plugin)
PYTEST ?= python -m pytest
PIP ?= pip
COVERAGE ?= $(shell which coverage)

PROTO_DIR := proto
PROTO_FILE := ${PROTO_DIR}/schema.proto 
PROTO_URL := https://raw.githubusercontent.com/codenotary/immudb/master/pkg/api/schema/schema.proto

SCHEMA_OUT_DIR := immudb/schema
GRPC_OUT_DIR := immudb/service
GOPATH ?= ${HOME}/go
PROTOC_INCLUDE_PATH := \-I${GOPATH}/pkg/mod \
		-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4/third_party/googleapis \
		-I${GOPATH}/pkg/mod/github.com/dgraph-io/badger/v2@v2.0.0-20200408100755-2e708d968e94 \
		-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4 \
		-I${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4/protoc-gen-swagger

.PHONY: ${PROTO_DIR}
${PROTO_DIR}:
	${PROTOC} -I${PROTO_DIR} ${PROTOC_INCLUDE_PATH} \
	${PROTO_FILE} \
	--python_out=. --grpc_out=. \
	--plugin=protoc-gen-grpc=/home/simone/sw/immu-py/bin/grpc_python_plugin
	
	
# 	python3 -m grpc_tools.protoc \
# 		${PROTOC_INCLUDE_PATH} \
# 		${PROTO_FILE} \
# 		--python_out=. \
# 		--grpc_python_out=.
# 	python3 -m grpc_tools.protoc \
# 		${PROTOC_INCLUDE_PATH} \
# 		${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4/protoc-gen-swagger/options/annotations.proto \
# 		--python_out=. \
# 		--grpc_python_out=.
# 	python3 -m grpc_tools.protoc \
# 		${PROTOC_INCLUDE_PATH} \
# 		${GOPATH}/pkg/mod/github.com/grpc-ecosystem/grpc-gateway@v1.14.4/protoc-gen-swagger/options/openapiv2.proto \
# 		--python_out=. \
# 		--grpc_python_out=.
	./fixup.sh

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
