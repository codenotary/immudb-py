import struct
from dataclasses import dataclass

from immu.schema import schema_pb2
from immu.service import schema_pb2_grpc
from immu.rootService import RootService
from immu import constants, proofs, item

@dataclass
class batchGetResponse:
    #keylist: schema_pb2.KeyList
    itemlist: schema_pb2.ItemList

def call(service: schema_pb2_grpc.ImmuServiceStub, rs: RootService, request: schema_pb2.KeyList):
    root = rs.get()

    rawRequest = schema_pb2.KeyList(
        keys = request.keys
    )

    msg = service.GetBatch(rawRequest)
    for k in msg.items:
        k.value=k.value[8:]
    return batchGetResponse(
        itemlist = msg
        #keylist = msg.KeyList,
        #itemlist = msg.ItemList
    )
