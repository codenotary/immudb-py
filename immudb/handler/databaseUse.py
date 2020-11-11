from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from google.protobuf.empty_pb2 import Empty

@dataclass
class dbUseResponse:
    reply: schema_pb2.UseDatabaseReply

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.Database): 
    
    msg = service.UseDatabase(request)
    return dbUseResponse(
       reply = msg
    )
