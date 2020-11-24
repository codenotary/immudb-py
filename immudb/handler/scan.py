from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.Key):
    msg = service.Scan(request)
    ret={}
    for i in msg.items:
        content=schema_pb2.Content()
        content.ParseFromString(i.value)
        ret[i.key]=content.payload
    return ret
