from dataclasses import dataclass

from immudb.schema import schema_pb2
from immudb.service import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class CurrentRootResponse:
    index: int
    root: bytes

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: None):
    root = rs.get()
    return CurrentRootResponse(index=root.index, root=root.root)
