
from immudb.grpc import schema_pb2_grpc
from google.protobuf import empty_pb2
from immudb.rootService import RootService

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService):
    resp= service.Health(empty_pb2.Empty())
    return resp.status
