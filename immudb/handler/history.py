from dataclasses import dataclass

from immudb.schema import schema_pb2
from immudb.service import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class historyResponseItem:
    key: bytes
    value: bytes
    timestamp: int
    index: int

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.Key):
    histo = service.History(request)
    histolist=[]
    for i in histo.items:
        content=schema_pb2.Content()
        content.ParseFromString(i.value)
        histolist.append( historyResponseItem(
            key=i.key, 
            value=content.payload,
            timestamp=content.timestamp,
            index=i.index,
            ))
    return histolist
