from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from google.protobuf.empty_pb2 import Empty

@dataclass
class dbListResponse:
    dblist: schema_pb2.DatabaseListResponse

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: None): 
    NoRequest=Empty()
    msg = service.DatabaseList(NoRequest)
    return dbListResponse(
       dblist = msg
    )
