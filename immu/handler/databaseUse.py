from dataclasses import dataclass

from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from immu import constants, proofs, item
from google.protobuf.empty_pb2 import Empty

@dataclass
class dbUseResponse:
    reply: schema_pb2.UseDatabaseReply

#def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.Database): 
def call(service: schema_pb2_grpc.ImmuServiceStub, request: schema_pb2.Database): 
    # root = rs.get()
    
    msg = service.UseDatabase(request)
    return dbUseResponse(
       reply = msg
    )
