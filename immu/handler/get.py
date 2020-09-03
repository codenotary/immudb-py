from dataclasses import dataclass

from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService

@dataclass
class GetResponse:
    value: bytes
    timestamp: int

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.Key):
    msg = service.GetSV(request)
    return GetResponse( value = msg.value.payload, timestamp=msg.value.timestamp )
