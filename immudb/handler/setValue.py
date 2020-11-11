from time import time
from dataclasses import dataclass

from immudb.schema import schema_pb2
from immudb.service import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class SetResponse:
    index: int

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.KeyValue):

    content=schema_pb2.Content(
        timestamp=int(time()),
        payload=request.value
        )
    
    skv=schema_pb2.KeyValue(key=request.key, value=content.SerializeToString())
    msg = service.SetSV(skv)

    return SetResponse(index = msg.index)
