from dataclasses import dataclass

from immudb.grpc import schema_pb2
from immudb.grpc import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class batchGetResponse:
    #keylist: schema_pb2.KeyList
    itemlist: schema_pb2.ItemList

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.KeyList):
    rawRequest = schema_pb2.KeyList(
        keys = request.keys
    )

    msg = service.GetBatch(rawRequest)
    ret={}
    for i in msg.items:
	    content=schema_pb2.Content()
	    content.ParseFromString(i.value)
	    ret[i.key]=content.payload
    return ret
