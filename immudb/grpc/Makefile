PROTO_DIR := proto
PROTO_FILE := ${PROTO_DIR}/schema.proto 
PROTOC_INCLUDE_PATH := 	-I${PROTO_DIR}

.PHONY: ${PROTO_DIR}
${PROTO_DIR}:
	python3 -m grpc_tools.protoc \
		${PROTO_FILE} \
		--proto_path=./proto \
		${PROTOC_INCLUDE_PATH} \
		--python_out=. \
		--grpc_python_out=.
	./fixup.sh

