from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from immudb import datatypes

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: None):
    root = rs.get()
    return datatypes.CurrentRootResponse(id=root.txId, hash=root.txHash)
