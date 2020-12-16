SHELL := /bin/bash


PYTEST ?= python -m pytest
PIP ?= pip
COVERAGE ?= $(shell which coverage)

all: init test

proto: immudb/grpc
	make -C immudb/grpc

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
