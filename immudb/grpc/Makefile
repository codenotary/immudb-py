PROTO_DIR := proto
# grpc-gateway https://github.com/grpc-ecosystem/grpc-gateway/tree/v1.16.0
# immudb https://github.com/codenotary/immudb/tree/master/pkg/api/schema


GRPC_GATEWAY := grpcgatewayrepo
PROTO_FILE := ${PROTO_DIR}/*.proto
PROTOC_INCLUDE_PATH_GOOGLE := 	-I${GRPC_GATEWAY} -I ${GRPC_GATEWAY}/third_party/googleapis   -I ${GRPC_GATEWAY}
PROTOC_INCLUDE_PATH := 	-I${PROTO_DIR}

.PHONY: ${PROTO_DIR}
${PROTO_DIR}:
	rm -rf ${GRPC_GATEWAY}
	git clone https://github.com/grpc-ecosystem/grpc-gateway.git -b v1.16.0 --depth=1 ${GRPC_GATEWAY}
	curl https://raw.githubusercontent.com/codenotary/immudb/master/pkg/api/schema/schema.proto -o ${PROTO_DIR}/schema.proto
	python3 -m grpc_tools.protoc \
		${PROTO_FILE} \
		--proto_path=./${PROTO_DIR} \
		${PROTOC_INCLUDE_PATH} \
		${PROTOC_INCLUDE_PATH_GOOGLE} \
		--python_out=. \
		--grpc_python_out=.
	touch __init__.py
	rm -rf ${GRPC_GATEWAY}
	./fixup.sh