from dataclasses import dataclass

from immudb.grpc import schema_pb2, schema_pb2_grpc
from immudb.rootService import RootService
from google.protobuf.empty_pb2 import Empty

@dataclass
class changePasswordResponse:
    reply: Empty

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.ChangePasswordRequest): 
    
    msg = service.ChangePassword(request)
    return changePasswordResponse(
       reply = msg
    )
