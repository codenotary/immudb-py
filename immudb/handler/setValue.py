from time import time
from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class SetResponse:
    index: int

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.KeyValue):

    content=schema_pb2.Content(
        timestamp=int(time()),
        payload=request.value
        )
    
    kv=schema_pb2.KeyValue(key=request.key, value=content.SerializeToString())
    msg = service.Set(kv)

    return SetResponse(index = msg.index)
