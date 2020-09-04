from dataclasses import dataclass

from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from google.protobuf.empty_pb2 import Empty

@dataclass
class dbCreateResponse:
    reply: Empty
    
def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.Database): 
    root = rs.get()
    
    msg = service.CreateDatabase(request)
    return dbCreateResponse(
       reply = msg
    )
