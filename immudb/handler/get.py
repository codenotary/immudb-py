from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class GetResponse:
    value: bytes
    timestamp: int

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.Key):
    msg = service.Get(request)
    content=schema_pb2.Content()
    content.ParseFromString(msg.value)
    return GetResponse( value = content.payload, timestamp=content.timestamp )
