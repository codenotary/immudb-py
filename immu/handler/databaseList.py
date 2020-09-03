from dataclasses import dataclass

from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from immu import constants, proofs, item
from google.protobuf.empty_pb2 import Empty

@dataclass
class dbListResponse:
    dblist: schema_pb2.DatabaseListResponse

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: None): 
    root = rs.get()
    NoRequest=Empty()
    msg = service.DatabaseList(NoRequest)
    return dbListResponse(
       dblist = msg
    )
