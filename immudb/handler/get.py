from dataclasses import dataclass

from immudb.schema import schema_pb2
from immudb.service import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class GetResponse:
    value: bytes
    timestamp: int

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.Key):
    msg = service.GetSV(request)
    return GetResponse( value = msg.value.payload, timestamp=msg.value.timestamp )
