from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class batchElement:
    tx: int
    key: bytes
    value: bytes

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, keys: list):
    request = schema_pb2.KeyListRequest(
        keys = keys#[schema_pb2_grpc.schema__pb2.Key(key=k) for k in keys]
    )
    msg = service.GetAll(request)
    ret={}
    for i in msg.entries:
        element=batchElement(
            tx=i.tx,
            key=i.key,
            value=i.value
            )
        ret[i.key]=element
    return ret
