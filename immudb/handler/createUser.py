from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from google.protobuf.empty_pb2 import Empty

@dataclass
class createUserResponse:
    reply: Empty

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.CreateUserRequest): 
    
    msg = service.CreateUser(request)
    return createUserResponse(
       reply = msg
    )
