from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService
from google.protobuf.empty_pb2 import Empty

@dataclass
class listUsersResponse:
    userlist: schema_pb2.UserList
    
def call(service: schema_pb2_grpc.ImmuServiceStub, request: None):
    NoRequest=Empty()
    msg = service.ListUsers(NoRequest)
    return listUsersResponse(
       userlist = msg
    )
    