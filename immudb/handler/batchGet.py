from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class batchGetResponse:
    itemlist: schema_pb2.Entries

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: list):
    rawRequest = schema_pb2.KeyListRequest(
        keys = request
    )

    msg = service.GetAll(rawRequest)
    ret={}
    for i in msg.items:
	    content=schema_pb2.Content()
	    content.ParseFromString(i.value)
	    ret[i.key]=content.payload
    return ret
