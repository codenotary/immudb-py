SHELL := /bin/bash

PROTOC ?= $(shell which protoc)
GRPC_PYTHON_PLUGIN ?= $(shell which grpc_python_plugin)
PYTEST ?= python -m pytest
PIP ?= pip

PROTO_DIR := proto
PROTO_FILE := ${PROTO_DIR}/schema.proto
PROTO_URL := https://raw.githubusercontent.com/codenotary/immudb/master/pkg/api/schema/schema.proto

SCHEMA_OUT_DIR := immu/schema
GRPC_OUT_DIR := immu/service


.PHONY: ${PROTO_DIR}
${PROTO_DIR}:
	rm -rf ${PROTO_DIR}
	mkdir -p ${PROTO_DIR} ${SCHEMA_OUT_DIR} ${GRPC_OUT_DIR}
	curl --silent -Lo ${PROTO_FILE} ${PROTO_URL}
	${PROTOC} -I ${PROTO_DIR} ${PROTO_FILE} \
		--python_out=${SCHEMA_OUT_DIR} \
		--grpc_out=${GRPC_OUT_DIR} \
		--plugin=protoc-gen-grpc=${GRPC_PYTHON_PLUGIN}

init:
	$(PIP) install -r requirements.txt --user

dev:
	$(PIP) install -r requirements-dev.txt

test:
	$(PYTEST) -vv --color=yes tests/
