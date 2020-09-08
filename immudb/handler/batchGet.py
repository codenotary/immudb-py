from dataclasses import dataclass

from immudb.schema import schema_pb2
from immudb.service import schema_pb2_grpc
from immudb.rootService import RootService

@dataclass
class batchGetResponse:
    #keylist: schema_pb2.KeyList
    itemlist: schema_pb2.ItemList

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.KeyList):
    rawRequest = schema_pb2.KeyList(
        keys = request.keys
    )

    msg = service.GetBatchSV(rawRequest)
    return batchGetResponse(
        itemlist = msg
        #keylist = msg.KeyList,
        #itemlist = msg.ItemList
    )
