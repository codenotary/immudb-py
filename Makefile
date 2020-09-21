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


.PHONY: ${PROTO_DIR}
${PROTO_DIR}:
	${PROTOC} -I ${PROTO_DIR} ${PROTO_FILE} \
		--python_out=${SCHEMA_OUT_DIR} \
		--grpc_out=${GRPC_OUT_DIR} \
		--plugin=protoc-gen-grpc=${GRPC_PYTHON_PLUGIN}

init:
	$(PIP) install -r requirements.txt --user

dev:
	$(PIP) install -r requirements-dev.txt --user

test:
	$(PYTEST) -vv --color=yes tests/

coverage:
	$(COVERAGE) run -m pytest tests
