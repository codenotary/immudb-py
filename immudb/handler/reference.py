from dataclasses import dataclass

from immudb.grpc import schema_pb2, schema_pb2_grpc
from immudb.rootService import RootService

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.Key):
    msg = service.Reference(request)
    return msg
