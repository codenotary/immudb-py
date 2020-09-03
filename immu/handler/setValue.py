from time import time
from dataclasses import dataclass

from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from immu import constants, proofs, item

@dataclass
class SetResponse:
    index: int

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.KeyValue):

    content=schema_pb2.Content(
        timestamp=int(time()),
        payload=request.value
        )
    
    skv=schema_pb2.StructuredKeyValue(key=request.key, value=content)
    msg = service.SetSV(skv)

    return SetResponse(index = msg.index)
